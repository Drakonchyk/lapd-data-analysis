import os
import pandas as pd
import streamlit as st
from databricks.sdk import WorkspaceClient
from src.ui import refresh_button

st.set_page_config(page_title="Ask the Data (Genie)", page_icon="✨", layout="wide")
st.title("✨ Ask the LAPD Crime Data (Genie)")
refresh_button()

SPACE_ID = os.getenv("GENIE_SPACE_ID", "").strip()

@st.cache_resource
def get_client() -> WorkspaceClient:
    return WorkspaceClient()

def statement_response_to_df(statement_response, max_rows: int = 100) -> pd.DataFrame | None:
    """
    Genie query results come back wrapped as a StatementResponse (same shape as SQL Statement Execution).
    """
    if not statement_response:
        return None
    manifest = getattr(statement_response, "manifest", None)
    result = getattr(statement_response, "result", None)
    if not manifest or not result:
        return None

    cols = []
    schema = getattr(manifest, "schema", None)
    if schema and getattr(schema, "columns", None):
        cols = [c.name for c in schema.columns]

    data = getattr(result, "data_array", None) or []
    if not cols:
        return pd.DataFrame(data)

    df = pd.DataFrame(data, columns=cols)
    return df.head(max_rows)

def render_genie_message(w: WorkspaceClient, space_id: str, msg) -> None:
    """
    Render one GenieMessage (attachments can include text + SQL query + suggested questions).
    """
    attachments = getattr(msg, "attachments", None) or []
    if not attachments:
        st.markdown("_No attachments returned._")
        return

    for att in attachments:
        if getattr(att, "text", None):
            st.markdown(att.text.content)

        if getattr(att, "query", None):
            q = att.query
            if getattr(q, "description", None):
                st.markdown(f"**Query explanation:** {q.description}")

            if getattr(q, "query", None):
                with st.expander("Show generated SQL", expanded=False):
                    st.code(q.query, language="sql")

            att_id = getattr(att, "attachment_id", None)
            if att_id:
                try:
                    qr = w.genie.get_message_attachment_query_result(
                        space_id=space_id,
                        conversation_id=msg.conversation_id,
                        message_id=msg.message_id,
                        attachment_id=att_id,
                    )
                    sr = getattr(qr, "statement_response", None)
                    df = statement_response_to_df(sr, max_rows=50)
                    if df is not None and not df.empty:
                        st.dataframe(df, use_container_width=True)
                except Exception as e:
                    st.caption(f"(Could not fetch query result preview: {e})")

        if getattr(att, "suggested_questions", None) and getattr(att.suggested_questions, "questions", None):
            with st.expander("Suggested follow-ups", expanded=False):
                for s in att.suggested_questions.questions:
                    if st.button(s, key=f"suggest_{msg.message_id}_{s}"):
                        st.session_state["pending_prompt"] = s

if not SPACE_ID:
    st.error(
        "GENIE_SPACE_ID is not set. Add a **Genie space** as an App resource and inject it via app.yaml:\n\n"
        "`env: - name: GENIE_SPACE_ID  valueFrom: genie-space`"
    )
    st.stop()

w = get_client()

if not hasattr(w, "genie"):
    st.error(
        "Your `databricks-sdk` is too old (WorkspaceClient has no `.genie`). "
        "Update `requirements.txt` (e.g. `databricks-sdk>=0.20.0`) and redeploy."
    )
    st.stop()

if "chat_history" not in st.session_state:
    st.session_state.chat_history = []

if "conversation_id" not in st.session_state:
    st.session_state.conversation_id = None

colA, colB = st.columns([1, 1])
with colA:
    if st.button("🧹 New conversation"):
        st.session_state.chat_history = []
        st.session_state.conversation_id = None
        st.rerun()

with colB:
    st.caption(f"Space: `{SPACE_ID}`")

for m in st.session_state.chat_history:
    with st.chat_message(m["role"]):
        st.markdown(m["content"])

prefill = st.session_state.pop("pending_prompt", None)

prompt = st.chat_input("Ask about crime trends, areas, victims, weapons…")
if prefill and not prompt:
    prompt = prefill

if prompt:
    st.session_state.chat_history.append({"role": "user", "content": prompt})
    with st.chat_message("user"):
        st.markdown(prompt)

    with st.chat_message("assistant"):
        with st.spinner("Genie is analyzing…"):
            try:
                if not st.session_state.conversation_id:
                    msg = w.genie.start_conversation_and_wait(space_id=SPACE_ID, content=prompt)
                    st.session_state.conversation_id = msg.conversation_id
                else:
                    msg = w.genie.create_message_and_wait(
                        space_id=SPACE_ID,
                        conversation_id=st.session_state.conversation_id,
                        content=prompt,
                    )

                render_genie_message(w, SPACE_ID, msg)

                st.session_state.chat_history.append(
                    {"role": "assistant", "content": "✅ Response generated (see above)."}
                )

            except Exception as e:
                st.error(f"Failed to call Genie: {e}")
