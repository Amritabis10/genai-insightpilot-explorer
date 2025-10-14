# GenAI InsightPilot Explorer

GenAI InsightPilot Explorer is a Streamlit experience for conversational analytics powered by Strands’ agentic tooling and Amazon Athena. Analysts can describe the insight they want, watch the agent craft precise SQL, review the results, and optionally render quick charts—all inside the chat flow.

- 🎯 Goal: Turn natural-language questions into data-backed answers.
- 🧠 Agent: A Strands Agent augmented with custom tools for database discovery, SQL generation, and visualization.
- 🛠️ Stack: Streamlit UI, Strands agent runtime, AWS Athena (via boto3), plus helper utilities for cost awareness and schema guidance.
- 📊 Features:
  * schema reference and example queries in the sidebar
  * step-by-step progress feedback while the agent works
  * inline SQL preview, Athena execution, and optional Vega-Lite charting
  * lightweight cost estimates for transparency
  * session history with tables, charts, and SQL blocks preserved

Perfect for anyone experimenting with GenAI-driven BI workflows or showcasing how agentic patterns streamline data exploration.
