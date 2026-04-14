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

# Render visible historical chat layout natively
for msg in st.session_state.messages:
    with st.chat_message(msg["role"]):
        st.markdown(msg["content"])
        # Optionally render expanders if retained historically
        if "trace" in msg:
            with st.expander("Pipeline Trace Details"):
                st.json(msg["trace"])

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
        
        with st.expander("Detailed Reasoning Trace & Extracted Logic Logs"):
            st.json(trace_steps)

    st.session_state.messages.append({
        "role": "assistant", 
        "content": summary_text,
        "trace": trace_steps
    })
