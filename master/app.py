import os
import uuid
import asyncio
from datetime import datetime, timezone

import streamlit as st
from dotenv import load_dotenv

from nexgen_shared.schemas import UserQuery
from src.orchestrator import MasterOrchestrator

# Load environment configuration dynamically parsing OPENAI limits natively
load_dotenv()

st.set_page_config(page_title="NexGen Orchestrator", page_icon="⚙️", layout="wide")
st.title("NexGen RCA Orchestrator 🧠")
st.markdown("Experimental Streamlit UI visualizing the internal state-machine pipeline paths and Reasoner trace evaluations.")

@st.cache_resource
def get_orchestrator():
    # Only load dependencies natively once
    return MasterOrchestrator()

orchestrator = get_orchestrator()

# Initialize Chat Memory Buffer
if "messages" not in st.session_state:
    st.session_state.messages = []
if "session_id" not in st.session_state:
    st.session_state.session_id = str(uuid.uuid4())

def render_trace_cards(trace_steps):
    for step in trace_steps:
        stage = step.get("stage", "working")
        with st.container(border=True):
            if stage == "session":
                st.info(f"**Session Management:** {step.get('msg', 'Loaded')}")
            elif stage == "intent":
                st.markdown("#### 🎯 Intent Classification")
                data = step.get("data", {})
                c1, c2, c3, c4 = st.columns(4)
                c1.metric("Logs Needed", str(data.get("logs_needed")))
                c2.metric("Docs Needed", str(data.get("docs_needed")))
                c3.metric("Quantitative", str(data.get("is_quantitative")))
                c4.metric("Qualitative", str(data.get("is_qualitative")))
            elif stage == "planner":
                st.markdown("#### 🗺️ DAG Planner")
                data = step.get("data", {})
                for node in data.get("nodes", []):
                    st.markdown(f"- **{node.get('action_type')}** (step: `{node.get('step_id')[:8]}`) -> deps: `{node.get('dependencies', [])}`")
            elif stage == "executor":
                st.markdown("#### ⚡ DAG Executor")
                metrics = step.get("metrics", {})
                st.success(f"Execution complete. Logs fetched: `{metrics.get('logs_fetched')}` | Docs fetched: `{metrics.get('docs_fetched')}`")
            elif stage == "reasoner":
                cycle = step.get("cycle", 1)
                st.markdown(f"#### 🧠 Reasoner (Cycle {cycle})")
                for i, hyp in enumerate(step.get("hypotheses", [])):
                    icon = "✅" if hyp.get("is_accepted") else "❌"
                    st.markdown(f"{icon} **Hypothesis {i+1}**: {hyp.get('description', '')} \n*(Support: `{hyp.get('supporting_evidence_count')}`, Contradictions: `{hyp.get('contradictions')}`)*")
            elif stage == "final":
                st.markdown("#### 🏁 Final RCA Synthesis")
                data = step.get("data", {})
                score = data.get("confidence", 0) * 100
                st.write(data.get("root_cause_summary", "Synthesis generated no summary."))
                
                # Protect Progress bar mapping bounds correctly natively
                safe_score = max(0.0, min(100.0, score))
                st.progress(safe_score / 100.0, text=f"Confidence: {safe_score:.1f}%")

# Render visible historical chat layout natively
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        # Optionally render expanders if retained historically
        if "trace" in msg:
            with st.expander("View Pipeline Breakdown", expanded=False):
                render_trace_cards(msg["trace"])

if prompt := st.chat_input("Ask NexGen to analyze a generic failure (e.g. 'Why did the gateway timeout at 09:50?')"):
    # Append strictly visible query state
    st.session_state.messages.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        status_placeholder = st.empty()
        trace_steps = []
        
        async def run_pipeline():
            query = UserQuery(
                query_id=str(uuid.uuid4()),
                raw_text=prompt,
                session_id=st.session_state.session_id,
                timestamp_utc=datetime.now(timezone.utc).isoformat()
            )
            
            async def progress_hook(update):
                stage = update.get("stage", "working")
                status_placeholder.markdown(f"**Executing Stage:** `{stage.upper()}`... ⚙️")
                trace_steps.append(update)
                await asyncio.sleep(0.01) # Small yield back to thread loop allowing UI flash hooks
            
            # Execute standard async process with explicit callbacks
            return await orchestrator.execute_query(query, progress_callback=progress_hook)

        rca_report_data = asyncio.run(run_pipeline())
        
        status_placeholder.empty()
        
        # Display Final Formatted Response
        summary_text = getattr(rca_report_data, "root_cause_summary", "Synthesis yielded no reliable summary.")
        confidence = getattr(rca_report_data, "confidence", 0.0)
        
        st.markdown(f"**RCA Summary:** {summary_text} (Confidence: {confidence*100:.1f}%)")
        
        with st.expander("View Pipeline Breakdown", expanded=True):
            render_trace_cards(trace_steps)

    st.session_state.messages.append({
        "role": "assistant", 
        "content": summary_text,
        "trace": trace_steps
    })
