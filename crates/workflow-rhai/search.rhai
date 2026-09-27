seq([tool("search", "q"), on_counter(tool("counter"), seq([spawn_agent("helper", "zero"), complete()]), fail())]);
