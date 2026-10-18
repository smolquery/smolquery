defmodule Smolquery.Burst.Policy do
  @moduledoc """
  When a scattered job bursts, and how many runners it takes (PL-77 D7).

  A job asks with `burst: false | :auto | n`. `false` is peers only. `:auto`
  bursts when the sealed scan is at least `min_scan_bytes`, with one runner
  per `target_bytes` of it, so a 16 GiB scan over a 256 MiB target asks for 64
  runners, the per-job cap, and a dashboard query over a few hundred MB asks
  for none. The target is a quarter of what a peer takes because a runner
  reads S3 at about a quarter of a pod's rate (1 GB/s against 4, T-683): on
  a 92 GB table 10 runners lost to the peers on every shape and 25 won on
  every scan shape, so a table at the 8 GiB threshold asks for 32, where a
  1 GiB target asked for 8. The count is a request:
  `Smolquery.QueryService.Scatter` deals sealed files round-robin over peers
  and runners, so a table of few large files gives fewer runner shards than
  asked, each at least one file, until byte-weighted targets land (T-686).
  An integer asks for exactly that many. Every answer is capped at
  `max_per_job` and at the pool's `max`: a runner past the pool's max would
  only queue for a checkout behind the first wave.

  The policy is pure. It answers from the request, the scan and the
  configuration, and when it refuses it says why, so the job can carry the
  reason as `burstRefused`. `:off` is not a refusal worth reporting: the job
  did not ask. Nor is `:auto` on a node without a pool, since `:auto` is
  every job's default; an integer on such a node is `:no_pool`, because the
  caller asked for something the deployment cannot give.

  ## The tree merge

  The policy also deals spilled runner partials into merge shards
  (`merge_groups/2`, T-685, T-700). A merge shard on a 10 GB Lambda
  re-aggregates partials at about 65 MB/s, a fifth of the coordinator's
  rate, and its time is linear in its bytes, so a shard is sized like a
  scan shard: partials in landing order up to `merge_target_bytes` (128
  MiB, about 2 s) and at most `merge_fan_in` of them. A shard of sixteen
  85 MB partials ran 15 to 21 s on the 92 GB bench and lost more than the
  coordinator's merge it replaced, so a partial that fits a shard with no
  other stays with the coordinator, whose merge of two large partials
  beats a lease. A level needs two shards: one shard moves the whole
  merge onto a runner at a fifth of the coordinator's rate and saves
  nothing. The tree does not run at all under `merge_min_bytes` of
  spilled partials, and a level runs only after the level before it
  halved the spilled bytes (`merge_shrank?/2`): a merge that finds few
  shared keys has nothing for the next level to save.

  The shards go to the peers first (`merge_shards/3`, T-701). A query pod
  merges at 200 to 400 MB/s against a Lambda's 65, with no lease and no
  bill, and its output comes back inline, so a peer takes
  `merge_peer_weight` groups in one shard (4, about what a pod does in the
  time a runner takes one) and the groups left over go to runners one
  each; `0` keeps every merge shard on runners. A level that would be one
  shard is not run: the coordinator merges those partials itself.
  """

  @type request :: false | :auto | non_neg_integer()

  @type config :: %{
          required(:enabled) => boolean(),
          required(:max) => pos_integer(),
          required(:max_per_job) => pos_integer(),
          required(:min_scan_bytes) => non_neg_integer(),
          required(:target_bytes) => pos_integer(),
          optional(atom()) => term()
        }

  @type refusal :: :off | :no_pool | {:under_threshold, non_neg_integer(), non_neg_integer()}

  @type merge_config :: %{
          required(:merge_fan_in) => pos_integer(),
          required(:merge_target_bytes) => pos_integer(),
          required(:merge_min_bytes) => non_neg_integer(),
          required(:merge_peer_weight) => non_neg_integer(),
          optional(atom()) => term()
        }

  @type merge_host(peer) :: :runner | {:peer, peer}

  @type partial :: %{required(:bytes) => non_neg_integer(), optional(atom()) => term()}

  @doc """
  How many runners a job that asked `request` over `scan_bytes` of sealed
  data gets under `config`, or why none.
  """
  @spec runners(request(), non_neg_integer(), config()) ::
          {:ok, pos_integer()} | {:none, refusal()}
  def runners(false, _scan_bytes, _config), do: {:none, :off}
  def runners(0, _scan_bytes, _config), do: {:none, :off}
  def runners(:auto, _scan_bytes, %{enabled: false}), do: {:none, :off}
  def runners(_count, _scan_bytes, %{enabled: false}), do: {:none, :no_pool}

  def runners(:auto, scan_bytes, %{min_scan_bytes: min}) when scan_bytes < min,
    do: {:none, {:under_threshold, scan_bytes, min}}

  def runners(:auto, scan_bytes, %{target_bytes: target} = config),
    do: {:ok, cap(ceil_div(scan_bytes, target), config)}

  def runners(count, _scan_bytes, config) when is_integer(count) and count > 0,
    do: {:ok, cap(count, config)}

  @doc """
  Whether `value` is a request a job may make.
  """
  @spec request?(term()) :: boolean()
  def request?(false), do: true
  def request?(:auto), do: true
  def request?(count) when is_integer(count) and count >= 0, do: true
  def request?(_other), do: false

  @doc """
  The reason of a refusal as a bare atom, for a label, a badge or a JSON
  field: `under_threshold` without the bytes it was judged against. `nil`
  stays `nil`, and an atom that is not a refusal passes through, since the
  scatter adds reasons of its own (`runners_unreachable`, `too_few_files`).
  """
  @spec reason(refusal() | atom() | nil) :: atom() | nil
  def reason(nil), do: nil
  def reason({reason, _scan_bytes, _threshold}), do: reason
  def reason(reason) when is_atom(reason), do: reason

  @doc """
  The merge shards one tree level deals `spilled` partials into under
  `config`, and the partials it leaves to the coordinator. Each shard is
  a list of at least two partials in the order given; a shard takes
  partials while it holds fewer than `merge_fan_in` and the next one
  keeps it within `merge_target_bytes`, so a partial that fits with no
  other is left. No level forms, and every partial is left, when
  `merge_fan_in` is under 2 (the tree is off), when the partials together
  are under `merge_min_bytes`, or when fewer than two shards form.
  """
  @spec merge_groups([partial()], merge_config()) :: {[[partial()]], [partial()]}
  def merge_groups(spilled, %{merge_fan_in: fan_in}) when fan_in < 2, do: {[], spilled}

  def merge_groups(spilled, %{merge_min_bytes: floor} = config) do
    if Enum.sum_by(spilled, & &1.bytes) < floor, do: {[], spilled}, else: deal(spilled, config)
  end

  @doc """
  The merge shards one level runs over `groups` (from `merge_groups/2`),
  each on a host: the peers take the first `merge_peer_weight` groups
  each, dealt as evenly as the peers allow, as one shard per peer; every
  group after that is a shard on a runner. None when fewer than two shards
  would run, since one shard moves the whole merge to one host and saves
  the coordinator nothing. A weight of `0` sends every group to a runner.
  """
  @spec merge_shards([[partial()]], [peer], merge_config()) :: [{merge_host(peer), [partial()]}]
        when peer: term()
  def merge_shards(groups, peers, %{merge_peer_weight: weight}) do
    on_peers = min(length(groups), weight * length(peers))
    {for_peers, for_runners} = Enum.split(groups, on_peers)

    shards =
      for_peers
      |> Enum.chunk_every(max(ceil_div(on_peers, max(length(peers), 1)), 1))
      |> Enum.zip_with(peers, fn chunk, peer -> {{:peer, peer}, List.flatten(chunk)} end)
      |> Enum.concat(Enum.map(for_runners, &{:runner, &1}))

    case shards do
      [_, _ | _] -> shards
      _one_or_none -> []
    end
  end

  @doc """
  Whether a level that merged `bytes_in` of partials into `bytes_out` earns
  the level after it: it did when it at least halved them. A level costs a
  lease and about 2 s of merge however little it saves, and the coordinator
  merges what is left at 200 to 400 MB/s, so a level that cut 2 GB by less
  than half saved under 3 s and the next would save less again.
  """
  @spec merge_shrank?(non_neg_integer(), non_neg_integer()) :: boolean()
  def merge_shrank?(bytes_in, bytes_out), do: bytes_out * 2 <= bytes_in

  defp deal(spilled, %{merge_fan_in: fan_in, merge_target_bytes: target}) do
    {shards, alone} =
      spilled
      |> Enum.reduce([], fn partial, groups -> place(partial, groups, fan_in, target) end)
      |> Enum.reverse()
      |> Enum.split_with(fn {_group, _bytes, count} -> count >= 2 end)

    case shards do
      [_, _ | _] -> {Enum.map(shards, &members/1), Enum.map(alone, &List.first(members(&1)))}
      _one_or_none -> {[], spilled}
    end
  end

  defp members({group, _bytes, _count}), do: Enum.reverse(group)

  defp place(partial, [{group, bytes, count} | rest], fan_in, target)
       when count < fan_in and bytes + partial.bytes <= target,
       do: [{[partial | group], bytes + partial.bytes, count + 1} | rest]

  defp place(partial, groups, _fan_in, _target), do: [{[partial], partial.bytes, 1} | groups]

  defp cap(count, %{max_per_job: per_job, max: max}),
    do: count |> min(per_job) |> min(max) |> max(1)

  defp ceil_div(bytes, target), do: div(bytes + target - 1, target)
end
