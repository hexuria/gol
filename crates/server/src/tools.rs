//! The tools a run gets (item 8a, decision 90A): `echo`, and the catalog
//! tools its agent's manifest names, from the catalog directory
//! (`GOL_CATALOG_DIR`). Each run loads the catalog itself, so its MCP
//! servers are its own and start on their first call.
use std::path::{Path, PathBuf};

use harness::{load_catalog, EchoTool, LoadedCatalog, Tool};

/// The catalog directory `GOL_CATALOG_DIR` names, if set and not empty.
pub fn catalog_dir_from_env() -> Option<PathBuf> {
    std::env::var_os("GOL_CATALOG_DIR")
        .filter(|dir| !dir.is_empty())
        .map(PathBuf::from)
}

/// Whether the catalog in `dir` loads: the server checks it once at start.
pub fn check_catalog(dir: &Path) -> Result<(), String> {
    load_catalog(dir)
        .map(|_| ())
        .map_err(|error| format!("{}: {error:?}", dir.display()))
}

/// A run's tools: `echo`, and the catalog's tools that `named` lists.
pub(crate) struct RunTools {
    catalog: Option<LoadedCatalog>,
    named: Vec<String>,
}

impl RunTools {
    /// Loads the catalog in `dir`, if any, for a run whose manifest names
    /// `named`. A catalog that no longer loads is the run's failure.
    pub(crate) fn load(dir: Option<&Path>, named: Vec<String>) -> Result<Self, String> {
        let catalog = dir
            .map(|dir| load_catalog(dir).map_err(|error| format!("catalog: {error:?}")))
            .transpose()?;
        Ok(Self { catalog, named })
    }

    /// `echo`, then each catalog tool whose name the manifest lists.
    pub(crate) fn tools(&self) -> Vec<&dyn Tool> {
        let mut tools: Vec<&dyn Tool> = vec![&EchoTool];
        if let Some(catalog) = &self.catalog {
            tools.extend(
                catalog
                    .tools()
                    .into_iter()
                    .filter(|tool| self.named.contains(&tool.descriptor().name)),
            );
        }
        tools
    }
}

#[cfg(test)]
mod tests {
    use super::*;

    fn names(tools: &RunTools) -> Vec<String> {
        tools
            .tools()
            .iter()
            .map(|tool| tool.descriptor().name)
            .collect()
    }

    /// A catalog with one MCP server, `local`, declaring `ping` and `pong`.
    /// Nothing starts it: the tools are only listed here.
    fn catalog() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("gol-tools-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("dir");
        std::fs::write(
            dir.join("harness.toml"),
            "[[mcp]]\nname = \"local\"\ncommand = \"true\"\n\n\
             [[mcp.tools]]\nname = \"ping\"\n\n[[mcp.tools]]\nname = \"pong\"\n",
        )
        .expect("catalog");
        dir
    }

    // Without a catalog a run has echo alone; with one, echo and the
    // catalog tools its manifest names, and no other.
    #[test]
    fn a_run_gets_echo_and_the_catalog_tools_its_manifest_names() {
        let bare = RunTools::load(None, vec!["ping".to_string()]).expect("load");
        assert_eq!(names(&bare), ["echo"]);
        let dir = catalog();
        let named = RunTools::load(Some(&dir), vec!["ping".to_string()]).expect("load");
        assert_eq!(names(&named), ["echo", "ping"]);
        let none = RunTools::load(Some(&dir), Vec::new()).expect("load");
        assert_eq!(names(&none), ["echo"]);
        assert!(check_catalog(&dir).is_ok());
    }

    // A catalog that does not load is an error, at start and for a run.
    #[test]
    fn a_catalog_that_does_not_load_is_an_error() {
        let missing = std::env::temp_dir().join(format!("gol-none-{}", uuid::Uuid::new_v4()));
        assert!(RunTools::load(Some(&missing), Vec::new()).is_err());
        assert!(check_catalog(&missing).is_err());
    }
}
