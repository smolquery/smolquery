defmodule Smolquery.Burst.PolicyTest do
  use ExUnit.Case, async: true

  alias Smolquery.Burst.Policy

  @gib 1_073_741_824
  @config %{
    enabled: true,
    max: 32,
    max_per_job: 16,
    min_scan_bytes: 8 * @gib,
    target_bytes: @gib
  }

  describe "runners/3" do
    test "false and 0 are peers only, and not a refusal worth reporting" do
      assert Policy.runners(false, 100 * @gib, @config) == {:none, :off}
      assert Policy.runners(0, 100 * @gib, @config) == {:none, :off}
    end

    test "auto bursts from the threshold up, one runner per target_bytes" do
      assert Policy.runners(:auto, 8 * @gib - 1, @config) ==
               {:none, {:under_threshold, 8 * @gib - 1, 8 * @gib}}

      assert Policy.runners(:auto, 8 * @gib, @config) == {:ok, 8}
      assert Policy.runners(:auto, 8 * @gib + 1, @config) == {:ok, 9}
    end

    test "an integer asks for exactly that many" do
      assert Policy.runners(3, 0, @config) == {:ok, 3}
      assert Policy.runners(3, 100 * @gib, @config) == {:ok, 3}
    end

    test "every answer is capped at max_per_job and at the pool's max" do
      assert Policy.runners(:auto, 100 * @gib, @config) == {:ok, 16}
      assert Policy.runners(100, 0, @config) == {:ok, 16}
      assert Policy.runners(100, 0, %{@config | max: 4}) == {:ok, 4}
      assert Policy.runners(:auto, 100 * @gib, %{@config | max_per_job: 64}) == {:ok, 32}
    end

    test "the default target gives a table at the threshold 32 runners and max_per_job caps 92 GB at 64 (T-683)" do
      config = %{@config | target_bytes: 268_435_456, max_per_job: 64, max: 1000}

      assert Policy.runners(:auto, 8 * @gib, config) == {:ok, 32}
      assert Policy.runners(:auto, 92 * 10 ** 9, config) == {:ok, 64}
    end

    test "a scan of zero bytes over a zero threshold still takes one runner" do
      assert Policy.runners(:auto, 0, %{@config | min_scan_bytes: 0}) == {:ok, 1}
    end

    test "a node without a pool refuses an integer, and quietly ignores auto" do
      off = %{@config | enabled: false}

      assert Policy.runners(4, 100 * @gib, off) == {:none, :no_pool}
      assert Policy.runners(:auto, 100 * @gib, off) == {:none, :off}
      assert Policy.runners(false, 100 * @gib, off) == {:none, :off}
    end
  end

  @mib 1_048_576
  @merge %{
    merge_fan_in: 16,
    merge_target_bytes: 128 * @mib,
    merge_min_bytes: 0,
    merge_peer_weight: 4
  }

  defp partials(sizes), do: Enum.with_index(sizes, fn bytes, i -> %{index: i, bytes: bytes} end)

  describe "merge_groups/2 (T-700)" do
    test "fills a shard up to merge_target_bytes in landing order" do
      spilled = partials(List.duplicate(25 * @mib, 12))

      assert {[first, second, third], []} = Policy.merge_groups(spilled, @merge)
      assert Enum.map(first, & &1.index) == [0, 1, 2, 3, 4]
      assert Enum.map(second, & &1.index) == [5, 6, 7, 8, 9]
      assert Enum.map(third, & &1.index) == [10, 11]
    end

    test "merge_fan_in caps a shard's inputs however small they are" do
      {groups, [%{index: 6}]} =
        Policy.merge_groups(partials(List.duplicate(@mib, 7)), %{@merge | merge_fan_in: 3})

      assert Enum.map(groups, &length/1) == [3, 3]
    end

    test "a partial that fits a shard with no other is left to the coordinator" do
      spilled = partials(List.duplicate(85 * @mib, 5))
      assert {[], ^spilled} = Policy.merge_groups(spilled, @merge)

      sizes = [60 * @mib, 60 * @mib, 300 * @mib, 60 * @mib, 60 * @mib, 60 * @mib]

      assert {[[%{index: 0}, %{index: 1}], [%{index: 3}, %{index: 4}]],
              [%{index: 2}, %{index: 5}]} =
               Policy.merge_groups(partials(sizes), @merge)
    end

    test "a level needs two shards" do
      spilled = partials(List.duplicate(25 * @mib, 6))
      assert {[], ^spilled} = Policy.merge_groups(spilled, @merge)
      assert {[_, _], []} = Policy.merge_groups(partials(List.duplicate(25 * @mib, 7)), @merge)
    end

    test "under merge_min_bytes the coordinator merges alone" do
      spilled = partials(List.duplicate(50 * @mib, 10))

      assert {[], ^spilled} =
               Policy.merge_groups(spilled, %{@merge | merge_min_bytes: 512 * @mib})

      assert {[_, _, _, _, _], []} =
               Policy.merge_groups(spilled, %{@merge | merge_min_bytes: 500 * @mib})

      assert {[], []} = Policy.merge_groups([], @merge)
    end

    test "a fan-in under 2 turns the tree off" do
      spilled = partials(List.duplicate(50 * @mib, 100))

      assert {[], ^spilled} = Policy.merge_groups(spilled, %{@merge | merge_fan_in: 1})
    end
  end

  describe "partition_groups/2 (T-701)" do
    defp buckets(sizes),
      do: Enum.with_index(sizes, fn bytes, p -> %{partition: p, bytes: bytes} end)

    test "packs whole partitions up to the target, one over the target alone" do
      sizes = [60 * @mib, 60 * @mib, 300 * @mib, 10 * @mib, 10 * @mib, 10 * @mib]

      assert [
               [%{partition: 0}, %{partition: 1}],
               [%{partition: 2}],
               [%{partition: 3}, %{partition: 4}, %{partition: 5}]
             ] =
               Policy.partition_groups(buckets(sizes), @merge)
    end

    test "under merge_min_bytes there is no level" do
      assert [] ==
               Policy.partition_groups(buckets([@mib, @mib]), %{
                 @merge
                 | merge_min_bytes: 3 * @mib
               })

      assert [[_, _]] =
               Policy.partition_groups(buckets([@mib, @mib]), %{
                 @merge
                 | merge_min_bytes: 2 * @mib
               })

      assert [] == Policy.partition_groups([], @merge)
    end
  end

  describe "merge_shards/3 (T-701)" do
    @peers [:a, :b, :c]

    defp groups(n), do: for(i <- 1..n, do: [%{index: i, bytes: @mib}, %{index: -i, bytes: @mib}])

    test "the peers take merge_peer_weight groups each, the rest go to runners one each" do
      shards = Policy.merge_shards(groups(15), @peers, @merge)

      assert [{{:peer, :a}, a}, {{:peer, :b}, b}, {{:peer, :c}, c} | runners] = shards
      assert Enum.map([a, b, c], &length/1) == [8, 8, 8]
      assert [{:runner, [_, _]}, {:runner, [_, _]}, {:runner, [_, _]}] = runners
    end

    test "fewer groups than the peers' share are dealt evenly over the peers" do
      assert [{{:peer, :a}, a}, {{:peer, :b}, b}, {{:peer, :c}, c}] =
               Policy.merge_shards(groups(5), @peers, @merge)

      assert Enum.map([a, b, c], &length/1) == [4, 4, 2]
    end

    test "a weight of 0 sends every group to a runner" do
      shards = Policy.merge_shards(groups(3), @peers, %{@merge | merge_peer_weight: 0})
      assert [{:runner, [_, _]}, {:runner, [_, _]}, {:runner, [_, _]}] = shards
    end

    test "a level that would be one shard is not run" do
      assert [] == Policy.merge_shards(groups(4), [:a], @merge)

      assert [{{:peer, :a}, _}, {:runner, _}] =
               Policy.merge_shards(groups(2), [:a], %{@merge | merge_peer_weight: 1})

      assert [{{:peer, :a}, _}, {{:peer, :b}, _}] =
               Policy.merge_shards(groups(2), @peers, %{@merge | merge_peer_weight: 2})

      assert [] == Policy.merge_shards([], @peers, @merge)
    end
  end

  describe "merge_shrank?/2" do
    test "a level earns the next when it at least halved the bytes" do
      assert Policy.merge_shrank?(2_000, 1_000)
      assert Policy.merge_shrank?(2_000, 600)
      refute Policy.merge_shrank?(2_000, 1_001)
      assert Policy.merge_shrank?(0, 0)
    end
  end

  describe "reason/1" do
    test "strips the bytes from an under_threshold refusal and passes atoms and nil through" do
      assert Policy.reason({:under_threshold, 10, 20}) == :under_threshold
      assert Policy.reason(:no_pool) == :no_pool
      assert Policy.reason(:runners_unreachable) == :runners_unreachable
      assert Policy.reason(nil) == nil
    end
  end

  describe "request?/1" do
    test "accepts false, auto and a count" do
      assert Policy.request?(false)
      assert Policy.request?(:auto)
      assert Policy.request?(0)
      assert Policy.request?(128)
    end

    test "refuses anything else" do
      refute Policy.request?(true)
      refute Policy.request?("auto")
      refute Policy.request?(-1)
      refute Policy.request?(nil)
    end
  end
end
