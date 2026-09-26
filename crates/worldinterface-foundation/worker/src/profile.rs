use crate::{nonempty, Result};
use serde::{Deserialize, Serialize};

pub const PROFILE_ID: &str = "process-managed-proof-1";
pub const DEFINITION_SHA256: &str =
    "6523baf56d14dafa4b199c7493dc030461cc061754c180b4c83006308e633986";
pub const ADR_APPROVAL: &str = "10a13b9c-cbfc-4469-9e03-9011adbdd507";

#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct ProfileBinding {
    pub schema: u32,
    pub profile_id: String,
    pub definition_sha256: String,
    pub decision_approval: String,
    pub reviewed_executable: bool,
    pub nonsensitive_inputs: bool,
    pub independent_worker_vm: bool,
    pub hostile_code_containment: bool,
}
impl ProfileBinding {
    pub fn process_proof() -> Self {
        Self {
            schema: 1,
            profile_id: PROFILE_ID.into(),
            definition_sha256: DEFINITION_SHA256.into(),
            decision_approval: ADR_APPROVAL.into(),
            reviewed_executable: true,
            nonsensitive_inputs: true,
            independent_worker_vm: false,
            hostile_code_containment: false,
        }
    }
    pub fn validate(&self) -> Result<()> {
        if self != &Self::process_proof() {
            return Err("profile definition or approval drift".into());
        }
        Ok(())
    }
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(rename_all = "snake_case")]
pub enum Mediation {
    Embedded,
    Delegated,
    External,
}
#[derive(Clone, Debug, PartialEq, Eq, Serialize, Deserialize)]
#[serde(deny_unknown_fields)]
pub struct Assurance {
    pub mediation: Mediation,
    pub enclosing_instance: Option<String>,
    pub independent_worker_vm_required: bool,
    pub hostile_code_containment_required: bool,
}
impl Assurance {
    pub fn validate(&self) -> Result<()> {
        if let Some(v) = &self.enclosing_instance {
            nonempty(v)?;
        }
        if self.independent_worker_vm_required || self.hostile_code_containment_required {
            return Err("process profile cannot provide independent containment".into());
        }
        Ok(())
    }
}
