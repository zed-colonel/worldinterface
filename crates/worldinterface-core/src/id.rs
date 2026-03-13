use std::fmt;
use std::str::FromStr;

use serde::{Deserialize, Serialize};
use uuid::Uuid;

macro_rules! define_id {
    ($name:ident, $doc:expr) => {
        #[doc = $doc]
        #[derive(
            Debug, Clone, Copy, PartialEq, Eq, PartialOrd, Ord, Hash, Serialize, Deserialize,
        )]
        #[serde(transparent)]
        pub struct $name(Uuid);

        impl Default for $name {
            fn default() -> Self {
                Self::new()
            }
        }

        impl $name {
            pub fn new() -> Self {
                Self(Uuid::new_v4())
            }
        }

        impl From<Uuid> for $name {
            fn from(uuid: Uuid) -> Self {
                Self(uuid)
            }
        }

        impl AsRef<Uuid> for $name {
            fn as_ref(&self) -> &Uuid {
                &self.0
            }
        }

        impl fmt::Display for $name {
            fn fmt(&self, f: &mut fmt::Formatter<'_>) -> fmt::Result {
                write!(f, "{}", self.0)
            }
        }

        impl FromStr for $name {
            type Err = uuid::Error;
            fn from_str(s: &str) -> Result<Self, Self::Err> {
                Ok(Self(s.parse::<Uuid>()?))
            }
        }
    };
}

define_id!(FlowId, "Identity of a named flow definition.");
define_id!(FlowRunId, "Identity of a flow execution instance.");
define_id!(NodeId, "Identity of a node within a FlowSpec.");
define_id!(StepRunId, "Identity of a step execution (wraps an ActionQueue RunId conceptually).");

/// Well-known NodeId for webhook trigger input data in ContextStore.
///
/// This is a deterministic UUID v5 derived from the URL namespace and a fixed name.
/// Flow nodes reference trigger data via `{{trigger.body}}`, `{{trigger.headers}}`, etc.
/// Internally, the parameter resolver reads from ContextStore at
/// `(flow_run_id, trigger_input_node_id())`.
pub fn trigger_input_node_id() -> NodeId {
    NodeId::from(Uuid::new_v5(&Uuid::NAMESPACE_URL, b"wi:trigger:input"))
}

#[cfg(test)]
mod tests {
    use std::collections::hash_map::DefaultHasher;
    use std::hash::{Hash, Hasher};

    use super::*;

    #[test]
    fn ids_are_unique() {
        let a = FlowRunId::new();
        let b = FlowRunId::new();
        assert_ne!(a, b);
    }

    #[test]
    fn id_roundtrips_through_json() {
        let id = FlowRunId::new();
        let json = serde_json::to_string(&id).unwrap();
        let back: FlowRunId = serde_json::from_str(&json).unwrap();
        assert_eq!(id, back);
    }

    #[test]
    fn id_display_is_uuid() {
        let id = NodeId::new();
        let display = id.to_string();
        Uuid::parse_str(&display).unwrap();
    }

    #[test]
    fn id_from_uuid_roundtrip() {
        let uuid = Uuid::new_v4();
        let id = FlowId::from(uuid);
        assert_eq!(*id.as_ref(), uuid);
    }

    #[test]
    fn equal_ids_from_same_uuid() {
        let uuid = Uuid::new_v4();
        let a = NodeId::from(uuid);
        let b = NodeId::from(uuid);
        assert_eq!(a, b);
    }

    #[test]
    fn id_from_str_roundtrip() {
        let id = FlowRunId::new();
        let s = id.to_string();
        let parsed: FlowRunId = s.parse().unwrap();
        assert_eq!(id, parsed);
    }

    #[test]
    fn id_from_str_rejects_invalid() {
        let result = "not-a-uuid".parse::<FlowRunId>();
        assert!(result.is_err());
    }

    #[test]
    fn trigger_input_node_id_is_deterministic() {
        let a = trigger_input_node_id();
        let b = trigger_input_node_id();
        assert_eq!(a, b);
    }

    #[test]
    fn trigger_input_node_id_is_not_nil() {
        let id = trigger_input_node_id();
        assert_ne!(*id.as_ref(), Uuid::nil());
    }

    #[test]
    fn equal_ids_have_same_hash() {
        let uuid = Uuid::new_v4();
        let a = StepRunId::from(uuid);
        let b = StepRunId::from(uuid);

        let hash = |id: &StepRunId| {
            let mut h = DefaultHasher::new();
            id.hash(&mut h);
            h.finish()
        };
        assert_eq!(hash(&a), hash(&b));
    }
}
