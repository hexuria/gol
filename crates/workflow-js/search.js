seq([tool("search", "q"), onCounter(tool("counter"), seq([spawnAgent("helper", "zero"), complete()]), fail())]);
