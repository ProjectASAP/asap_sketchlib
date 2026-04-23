# APIs Index

This is the canonical API entry point for `asap_sketchlib`.

## Core Sketch APIs

- [CountMin Sketch](./api/api_countmin.md) - `Ready`
  - Reference: Cormode & Muthukrishnan, "An Improved Data Stream Summary: The Count-Min Sketch and its Applications," PODS 2003. [https://dl.acm.org/doi/10.1145/762471.762473](https://dl.acm.org/doi/10.1145/762471.762473)
- [Count Sketch](./api/api_count_sketch.md) - `Ready`
  - Reference: Charikar, Chen & Farach-Colton, "Finding Frequent Items in Data Streams," ICALP 2002. [https://dl.acm.org/doi/10.1007/3-540-45465-9_59](https://dl.acm.org/doi/10.1007/3-540-45465-9_59)
- [HyperLogLog](./api/api_hyperloglog.md) - `Ready`
  - `Classic` variant: Flajolet, Fusy, Gandouet & Meunier, "HyperLogLog: the analysis of a near-optimal cardinality estimation algorithm," AOFA 2007. [https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf](https://algo.inria.fr/flajolet/Publications/FlFuGaMe07.pdf)
  - `ErtlMLE` variant (improved estimator): Ertl, "New cardinality estimation algorithms for HyperLogLog sketches," arXiv:1702.01284. [https://arxiv.org/abs/1702.01284](https://arxiv.org/abs/1702.01284)
  - `HIP` variant: Lang, "Back to the Future: an Even More Nearly Optimal Cardinality Estimation Algorithm," arXiv:1708.06839. [https://arxiv.org/abs/1708.06839](https://arxiv.org/abs/1708.06839)
- [KLL](./api/api_kll.md) - `Ready`
  - Reference: Karnin, Lang & Liberty, "Optimal Quantile Approximation in Streams," FOCS 2016. [https://arxiv.org/abs/1603.05346](https://arxiv.org/abs/1603.05346)
- [DDSketch](./api/api_ddsketch.md) - `Ready`
  - Reference: Masson, Rim & Lee, "DDSketch: A Fast and Fully-Mergeable Quantile Sketch with Relative-Error Guarantees," VLDB 2019. [https://arxiv.org/abs/1908.10693](https://arxiv.org/abs/1908.10693)
- [CMSHeap](./api/api_cms_heap.md) - `Ready`
- [CSHeap](./api/api_cs_heap.md) - `Ready`
- [Elastic](./api/api_elastic.md) - `Unstable`
  - Reference: Chen et al., "Elastic Sketch: Adaptive and Fast Network-wide Measurements," SIGCOMM 2018. [https://dl.acm.org/doi/10.1145/3230543.3230544](https://dl.acm.org/doi/10.1145/3230543.3230544)
- [Coco](./api/api_coco.md) - `Unstable`
  - Reference: CocoSketch paper. [https://dl.acm.org/doi/10.1145/3452296.3472892](https://dl.acm.org/doi/10.1145/3452296.3472892)
- [UniformSampling](./api/api_uniform_sampling.md) - `Unstable`
- [KMV](./api/api_kmv.md) - `Unstable`
  - Reference: KMV-related distinct-counting paper. [https://dl.acm.org/doi/10.1145/1247480.1247504](https://dl.acm.org/doi/10.1145/1247480.1247504)

## Framework APIs

- [Hydra](./api/api_hydra.md) - `Ready`
  - Reference: Manousis et al., VLDB 2022. [https://vldb.org/pvldb/vol15/p3249-manousis.pdf](https://vldb.org/pvldb/vol15/p3249-manousis.pdf)
- [HashSketchEnsemble](./api/api_hashlayer.md) - `Ready`
- [UnivMon](./api/api_univmon.md) - `Ready`
  - Reference: Liu et al., "One Sketch To Rule Them All: Rethinking Network Flow Monitoring with UnivMon," SIGCOMM 2016. [https://dl.acm.org/doi/10.1145/2934872.2934906](https://dl.acm.org/doi/10.1145/2934872.2934906)
- [UnivMon Optimized](./api/api_univmon_optimized.md) - `Ready`
- [NitroBatch](./api/api_nitrobatch.md) - `Ready`
  - Reference: NitroSketch paper. [https://dl.acm.org/doi/10.1145/3341302.3342076](https://dl.acm.org/doi/10.1145/3341302.3342076)
- [ExponentialHistogram](./api/api_exponential_histogram.md) - `Ready`
  - Reference: Datar, Gionis, Indyk & Motwani, "Maintaining Stream Statistics over Sliding Windows," SIAM J. Computing 2002. [https://dl.acm.org/doi/10.1137/S0097539701398363](https://dl.acm.org/doi/10.1137/S0097539701398363)
  - Related implementation context: PromSketch paper. [https://dl.acm.org/doi/pdf/10.14778/3742728.3742732](https://dl.acm.org/doi/pdf/10.14778/3742728.3742732)
- [EHSketchList](./api/api_ehsketchlist.md) - `Ready`
- [EHUnivOptimized](./api/api_ehunivoptimized.md) - `Unstable`
- [OctoSketch](./api/api_octo.md) - `Ready`
  - Reference: OctoSketch paper. [https://www.usenix.org/conference/nsdi24/presentation/zhang-yinda](https://www.usenix.org/conference/nsdi24/presentation/zhang-yinda)
## Common Utility APIs

- [Common Module API (Canonical)](./api/api_common.md) - `Shared`
- [Common Input Types](./api/api_common_input.md) - `Shared`
- [Common Hash Utilities](./api/api_common_hash.md) - `Shared`
- [Common Heap Utilities](./api/api_common_heap.md) - `Shared`
- [Common Structures](./api/api_common_structures.md) - `Shared`

## Notes

- Shared enums and foundational types are canonical in [Common Module API](./api/api_common.md).
- Unstable APIs remain visible with explicit caveats until migration completes.
- `Unstable` means the API is available but may change in upcoming minor releases; release notes will include migration guidance for breaking changes.
