"""
graph.py — The LangGraph multi-agent workflow.

This file wires the 4 agents together into a directed graph with
a conditional feedback loop.

The graph looks like this:

  START
    |
    v
  inspector ──────────────────────> analyst
                                       |
                                       v
                                     fixer
                                       |
                                       v
                                     critic
                                       |
                          ┌────────────┴────────────┐
                          │ is_confident?            │
                          │                         │
                      YES v                     NO  v
                         END               analyst (loop back)
                                      (max MAX_ITERATIONS then END)

Key LangGraph concepts used here:
  - StateGraph: a graph where every node reads/writes to a shared state dict
  - add_node: registers a Python function as a graph node
  - add_edge: draws a directed arrow between two nodes
  - add_conditional_edges: draws an arrow that branches based on a function's return value
  - set_entry_point: marks which node runs first
  - compile(): finalises the graph and returns a runnable object
"""

from langgraph.graph import StateGraph, END
from agents.inspector import run_inspector
from agents.analyst import run_analyst
from agents.fixer import run_fixer
from agents.critic import run_critic

# Stop looping after this many Analyst->Fixer->Critic attempts.
# Raised from 3 to 6 to give the agent more chances to get it right.
MAX_ITERATIONS = 6


def route_after_critic(state: dict) -> str:
    """
    Router function — called after every Critic run.
    Returns the name of the NEXT node to run.

    - Critic accepted  -> END (PR is open, we're done)
    - Max iterations   -> END (give up, log a summary)
    - Otherwise        -> loop back to Analyst with full feedback history
    """
    critique = state.get("critique")
    iteration = state.get("iteration", 0)

    if critique and critique.is_confident:
        print("\n✅ [Graph] Critic accepted the fix. Graph complete.")
        return "end"

    if iteration >= MAX_ITERATIONS:
        print(f"\n⚠️ [Graph] Reached max iterations ({MAX_ITERATIONS}). Ending graph.")
        return "end"

    print(
        f"\n🔄 [Graph] Critic rejected fix. "
        f"Looping back to Analyst (iteration {iteration}/{MAX_ITERATIONS})..."
    )
    return "analyst"


def build_graph():
    """
    Build and compile the LangGraph agent graph.
    Returns a compiled graph that can be invoked with an initial state dict.
    """
    graph = StateGraph(dict)

    graph.add_node("inspector", run_inspector)
    graph.add_node("analyst", run_analyst)
    graph.add_node("fixer", run_fixer)
    graph.add_node("critic", run_critic)

    graph.set_entry_point("inspector")

    graph.add_edge("inspector", "analyst")
    graph.add_edge("analyst", "fixer")
    graph.add_edge("fixer", "critic")

    graph.add_conditional_edges(
        source="critic",
        path=route_after_critic,
        path_map={
            "analyst": "analyst",
            "end": END,
        }
    )

    return graph.compile()


# Build the graph once at module load time (reused for every pipeline failure)
agent_graph = build_graph()
