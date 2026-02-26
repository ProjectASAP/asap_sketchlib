//! Exponential-Histogram node adapter for the node orchestrator.
//! Wraps EH sketches behind the `OrchestratorNode` interface.

use crate::{
    SketchInput,
    sketch_framework::{EHSketchList, ExponentialHistogram},
};

use super::{NodeInsert, NodeQuery, OrchestratorNode};

pub struct EhNode<Q>
where
    Q: Fn(&EHSketchList, &NodeQuery<'_>) -> Result<f64, &'static str>,
{
    eh: ExponentialHistogram,
    query_fn: Q,
}

impl<Q> EhNode<Q>
where
    Q: Fn(&EHSketchList, &NodeQuery<'_>) -> Result<f64, &'static str>,
{
    pub fn new(eh: ExponentialHistogram, query_fn: Q) -> Self {
        Self { eh, query_fn }
    }
}

impl<Q> OrchestratorNode for EhNode<Q>
where
    Q: Fn(&EHSketchList, &NodeQuery<'_>) -> Result<f64, &'static str>,
{
    fn kind(&self) -> &'static str {
        "EH"
    }

    fn insert(&mut self, _input: &SketchInput) {
        // EH requires time; use insert_ex with NodeInsert::Eh
    }

    fn insert_ex(&mut self, input: &NodeInsert<'_>) -> Result<(), &'static str> {
        match input {
            NodeInsert::Eh { time, value } => {
                self.eh.update(*time, value);
                Ok(())
            }
            _ => Err("Input type not supported by EH node"),
        }
    }

    fn query(&self, req: &NodeQuery<'_>) -> Result<f64, &'static str> {
        match req {
            NodeQuery::EhInterval { t1, t2, query } => {
                let merged = self
                    .eh
                    .query_interval_merge(*t1, *t2)
                    .ok_or("EH has no data in interval")?;
                (self.query_fn)(&merged, query.as_ref())
            }
            _ => Err("Query type not supported by EH node"),
        }
    }
}
