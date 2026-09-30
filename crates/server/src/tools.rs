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

/// The built-in tool every run has.
const ECHO: &str = "echo";

/// A run's tools: `echo`, and the catalog's tools that `named` lists.
pub(crate) struct RunTools {
    catalog: Option<LoadedCatalog>,
    named: Vec<String>,
}

impl RunTools {
    /// Loads the catalog in `dir`, if any, for a run whose manifest names
    /// `named`. A run that names no catalog tool does not load it, so a
    /// broken catalog touches only the runs that need it; for those it is an
    /// error the caller reports.
    pub(crate) fn load(dir: Option<&Path>, named: Vec<String>) -> Result<Self, String> {
        let needs = named.iter().any(|name| name != ECHO);
        let catalog = match dir {
            Some(dir) if needs => Some(
                load_catalog(dir).map_err(|error| format!("the catalog did not load: {error}"))?,
            ),
            _ => None,
        };
        Ok(Self { catalog, named })
    }

    /// `echo`, then each catalog tool whose name the manifest lists. A
    /// catalog tool named `echo` is left out: one name, one tool.
    pub(crate) fn tools(&self) -> Vec<&dyn Tool> {
        let mut tools: Vec<&dyn Tool> = vec![&EchoTool];
        if let Some(catalog) = &self.catalog {
            tools.extend(catalog.tools().into_iter().filter(|tool| {
                let name = tool.descriptor().name;
                name != ECHO && self.named.contains(&name)
            }));
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

    /// A catalog with one MCP server, `local`, declaring `ping`, `pong` and
    /// an `echo` of its own. Nothing starts it: the tools are only listed.
    fn catalog() -> PathBuf {
        let dir = std::env::temp_dir().join(format!("gol-tools-{}", uuid::Uuid::new_v4()));
        std::fs::create_dir_all(&dir).expect("dir");
        std::fs::write(
            dir.join("harness.toml"),
            "[[mcp]]\nname = \"local\"\ncommand = \"true\"\n\n\
             [[mcp.tools]]\nname = \"ping\"\n\n[[mcp.tools]]\nname = \"pong\"\n\n\
             [[mcp.tools]]\nname = \"echo\"\n",
        )
        .expect("catalog");
        dir
    }

    fn owned(names: &[&str]) -> Vec<String> {
        names.iter().map(|name| (*name).to_string()).collect()
    }

    // Without a catalog a run has echo alone; with one, echo and the
    // catalog tools its manifest names, and no other.
    #[test]
    fn a_run_gets_echo_and_the_catalog_tools_its_manifest_names() {
        let bare = RunTools::load(None, owned(&["ping"])).expect("load");
        assert_eq!(names(&bare), ["echo"]);
        let dir = catalog();
        let named = RunTools::load(Some(&dir), owned(&["ping"])).expect("load");
        assert_eq!(names(&named), ["echo", "ping"]);
        let none = RunTools::load(Some(&dir), Vec::new()).expect("load");
        assert_eq!(names(&none), ["echo"]);
        assert!(check_catalog(&dir).is_ok());
    }

    // A catalog tool named `echo` is left out: the run's echo is the
    // built-in one, once.
    #[test]
    fn a_catalog_echo_is_left_out() {
        let dir = catalog();
        let tools = RunTools::load(Some(&dir), owned(&["echo", "ping"])).expect("load");
        assert_eq!(names(&tools), ["echo", "ping"]);
    }

    // A catalog that does not load is an error at start, and for a run that
    // names a catalog tool; a run that names none (or only echo) does not
    // load it, so it runs.
    #[test]
    fn a_catalog_that_does_not_load_fails_only_the_runs_that_need_it() {
        let missing = std::env::temp_dir().join(format!("gol-none-{}", uuid::Uuid::new_v4()));
        assert!(check_catalog(&missing).is_err());
        assert!(RunTools::load(Some(&missing), owned(&["ping"])).is_err());
        for named in [Vec::new(), owned(&["echo"])] {
            let tools = RunTools::load(Some(&missing), named).expect("not loaded");
            assert_eq!(names(&tools), ["echo"]);
        }
    }
}
