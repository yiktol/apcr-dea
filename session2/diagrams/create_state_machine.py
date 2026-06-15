"""
Generate Step Functions state machine flowchart using Graphviz dot.
"""
import os
import subprocess

os.chdir(os.path.dirname(os.path.abspath(__file__)))

DOT_SOURCE = """
digraph StateMachine {
    rankdir=TB;
    bgcolor="white";
    pad="0.3";
    nodesep="0.5";
    ranksep="0.6";
    
    node [fontname="Helvetica Neue", fontsize="12", style="filled,rounded", shape="box"];
    edge [fontname="Helvetica Neue", fontsize="10", color="#555555", penwidth="2.0"];

    // Start/End nodes
    START [label="Start", shape="circle", width="0.6", height="0.6", 
           fillcolor="#232F3E", fontcolor="white", fixedsize=true];
    END [label="End", shape="doublecircle", width="0.6", height="0.6",
         fillcolor="#232F3E", fontcolor="white", fixedsize=true];

    // Process nodes
    VALIDATE [label="Validate Data\\n(Lambda)", fillcolor="#1565C0", fontcolor="white"];
    GLUE [label="Run Glue ETL\\n(sync - wait for completion)", fillcolor="#1565C0", fontcolor="white"];
    NOTIFY_OK [label="Notify Success\\n(Lambda → SNS)", fillcolor="#4CAF50", fontcolor="white"];
    NOTIFY_FAIL [label="Notify Failure\\n(Lambda → SNS)", fillcolor="#C62828", fontcolor="white"];

    // Decision node
    RETRY [label="Retry?", shape="diamond", fillcolor="#FFF8E1", 
           fontcolor="#E65100", width="1.2", height="0.8"];

    // Error node
    CATCH [label="Catch Error", fillcolor="#FFCDD2", fontcolor="#B71C1C"];

    // Skip node
    END_SKIP [label="End\\n(No Data)", shape="doublecircle", width="0.6", height="0.6",
              fillcolor="#9E9E9E", fontcolor="white", fixedsize=true];

    // Edges
    START -> VALIDATE [penwidth="2.5"];
    VALIDATE -> GLUE [label="  valid  ", color="#4CAF50", fontcolor="#2E7D32", penwidth="2.5"];
    VALIDATE -> END_SKIP [label="  skip / invalid  ", color="#9E9E9E", style="dashed"];
    GLUE -> NOTIFY_OK [label="  success  ", color="#4CAF50", fontcolor="#2E7D32", penwidth="2.5"];
    GLUE -> CATCH [label="  error  ", color="#C62828", fontcolor="#C62828", penwidth="2"];
    CATCH -> RETRY [penwidth="2"];
    RETRY -> GLUE [label="  yes (max 2)  ", color="#FF9900", fontcolor="#E65100", penwidth="2"];
    RETRY -> NOTIFY_FAIL [label="  exhausted  ", color="#C62828", fontcolor="#C62828", penwidth="2"];
    NOTIFY_OK -> END [penwidth="2.5"];
    NOTIFY_FAIL -> END [penwidth="2"];
}
"""

# Write dot file
with open("04_state_machine.dot", "w") as f:
    f.write(DOT_SOURCE)

# Render to PNG
subprocess.run(
    ["dot", "-Tpng", "-Gdpi=200", "-o", "04_state_machine.png", "04_state_machine.dot"],
    check=True,
)

# Clean up dot file
os.remove("04_state_machine.dot")

print("✅ 04_state_machine.png generated")
