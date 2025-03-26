import streamlit as st
import os
import json
import pandas as pd
from typing import List, Dict, Any, Tuple
import time # For st.toast duration

# --- Page Configuration ---
st.set_page_config( page_title="Human Annotation Tool", page_icon="📝", layout="wide")

# --- Custom CSS (Keep as before) ---
st.markdown("""
<style>
    /* General */
    .stApp { }
    .main .block-container { padding: 1rem 2rem; }
    /* Headers */
    .main-header { text-align: center; margin-bottom: 15px; font-size: 2rem; }
    .sub-header { margin-bottom: 10px; font-size: 1.2rem; }
    .annotator-info { text-align: center; font-size: 0.9rem; margin-bottom: 15px; border: 1px solid #aaa; padding: 5px; border-radius: 5px; }
    /* Boxes & Containers */
    .box { padding: 15px; border-radius: 8px; margin-bottom: 15px; border: 1px solid #aaa; }
    .question-box { }
    .answer-box { }
    /* Dataframe */
    div[data-testid="stDataFrame"] { }
    /* Agent Info */
    .agent-info-container { }
    .verification-box { padding: 10px; border-radius: 5px; margin-bottom: 15px; font-weight: bold; text-align: center; }
    .verified-true { background-color: #1e4620; color: #d4edda; border: 1px solid #3c763d; }
    .verified-false { background-color: #5a1e21; color: #f8d7da; border: 1px solid #a94442; }
    /* Buttons & Inputs */
    .stButton>button { width: 100%; border-radius: 5px; padding: 8px 0; }
    div[data-testid="column"] .stButton button { }
    /* Text Area for Answer */
    div[data-testid="stTextArea"] textarea { font-family: monospace; min-height: 120px; }
    /* Progress Bar */
    .stProgress > div > div > div {height: 12px; border-radius: 5px;}
    /* Metrics */
    .stMetric { border: 1px solid #aaa; border-radius: 8px; padding: 10px 15px; margin-bottom: 10px; }
    .stMetric > label { font-weight: bold; }
    /* Expander */
    .stExpander { border: 1px solid #aaa !important; border-radius: 8px; margin-bottom: 15px; }
    .stExpander > details > summary { padding: 10px 15px; }
    .stExpander > details[open] > summary { border-bottom: 1px solid #aaa; }
    .stExpander > details > div { padding: 15px; }
</style>
""", unsafe_allow_html=True)


# --- Helper Functions for Answer JSON Formatting ---

def answer_dict_to_json_string(answer_dict: Dict | None) -> str:
    """Converts the answer dictionary to a pretty-printed JSON string."""
    if not answer_dict:
        # Provide a default structure if missing, for editing purposes
        return json.dumps({"data": []}, indent=2)
    try:
        # Ensure 'data' key exists, default to empty list if not
        if 'data' not in answer_dict:
             answer_dict['data'] = []
        return json.dumps(answer_dict, indent=2, ensure_ascii=False)
    except Exception as e:
        st.warning(f"Could not format answer dictionary to JSON: {e}")
        # Fallback: simple string representation
        return str(answer_dict)

def json_string_to_answer_dict(text: str) -> Tuple[Dict | None, bool]:
    """
    Parses a JSON string back into an answer dictionary.
    Returns (parsed_dict, is_valid).
    """
    if not text.strip():
        # Consider empty string as valid, representing an empty answer
        return {"data": []}, True
    try:
        parsed_dict = json.loads(text)
        # Basic validation: is it a dictionary? Does it have 'data'? Is 'data' a list?
        if isinstance(parsed_dict, dict) and 'data' in parsed_dict and isinstance(parsed_dict['data'], list):
            # Optional: further check if 'data' contains lists? Depends on strictness needed.
            # Example check: all(isinstance(item, list) for item in parsed_dict['data'])
            return parsed_dict, True
        else:
             st.error("Invalid JSON structure for answer. Expected format like: {\"data\": [...]}")
             return None, False
    except json.JSONDecodeError as e:
        st.error(f"Invalid JSON format in answer text: {e}")
        return None, False
    except Exception as e:
        st.error(f"Error parsing answer JSON: {e}")
        return None, False

# --- Utility Functions (Load/Save JSON, Find Files - Keep as before) ---
@st.cache_data(show_spinner=False)
def load_json_file(filepath: str) -> Dict | List | None:
    try:
        with open(filepath, 'r', encoding='utf-8') as f: return json.load(f)
    except FileNotFoundError: st.error(f"Error: File not found at {filepath}"); return None
    except json.JSONDecodeError: st.error(f"Error: Could not decode JSON from {filepath}."); return None
    except Exception as e: st.error(f"Error loading file {filepath}: {str(e)}"); return None

def save_json_file(data: Any, filepath: str) -> bool:
    try:
        os.makedirs(os.path.dirname(filepath), exist_ok=True)
        with open(filepath, 'w', encoding='utf-8') as f: json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e: st.error(f"Error saving file {filepath}: {str(e)}"); return False

@st.cache_data(show_spinner=False)
def find_folders(directory: str) -> List[str]:
    if not os.path.exists(directory): return []
    try: return sorted([d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))])
    except Exception as e: st.error(f"Error listing directories in {directory}: {str(e)}"); return []

@st.cache_data(show_spinner=False)
def find_subset_files(directory: str) -> Tuple[str | None, str | None]:
    table_files = []
    try:
        for file in os.listdir(directory):
            if file.endswith("_subset.json"): table_files.append(os.path.join(directory, file))
        if len(table_files) >= 2: table_files.sort(); return table_files[0], table_files[1]
        elif len(table_files) == 1: return table_files[0], None
        else: return None, None
    except Exception as e: st.error(f"Error finding subset files in {directory}: {str(e)}"); return None, None

@st.cache_data(show_spinner=False)
def find_qa_file(directory: str) -> str | None:
    try:
        for file in os.listdir(directory):
            if "all_qa_pairs" in file and file.endswith(".json"): return os.path.join(directory, file)
        for file in os.listdir(directory):
             if ("qa_pairs" in file or "qa" in file) and file.endswith(".json"): return os.path.join(directory, file)
        return None
    except Exception as e: st.error(f"Error finding QA file in {directory}: {str(e)}"); return None

# --- UI Display Functions ---
def display_table(table_data: Dict | None, title: str) -> None:
    st.markdown(f"###### {title}")
    if not table_data: st.warning("No table data available."); return
    table_name = table_data.get('table_name', 'Unknown Table'); columns = table_data.get('columns', []); data = table_data.get('data', [])
    if columns and data:
        try: df = pd.DataFrame(data, columns=columns); st.dataframe(df, use_container_width=True, height=300)
        except ValueError as e: st.error(f"DataFrame Error ({table_name}): {e}.")
        except Exception as e: st.error(f"Display Error ({table_name}): {e}")
    elif columns: st.warning(f"Table '{table_name}' has columns but no data.")
    else: st.warning(f"Table data for '{table_name}' is incomplete.")

def display_agent_votes_summary(verification: Dict | None):
    if not verification: st.info("No agent verification data."); return
    valid_votes=verification.get("valid_votes",0); multiple_tables_votes=verification.get("multiple_tables_votes",0)
    total_votes=verification.get("total_votes",0); avg_score=verification.get("average_score",0.0)
    st.markdown("###### Agent Evaluation Summary")
    c1,c2,c3=st.columns(3)
    with c1: st.metric("Valid Votes", f"{valid_votes}/{total_votes}")
    with c2: st.metric("Multi-Table", f"{multiple_tables_votes}/{total_votes}")
    with c3: st.metric("Avg Score", f"{avg_score:.1f}/10" if isinstance(avg_score,(int,float)) else "N/A")

def display_agent_comments_reasoning(qa_pair: Dict):
    verification = qa_pair.get("verification", {}); comments = verification.get("verification_comments", [])
    reasoning_steps = qa_pair.get("reasoning_steps", [])
    with st.expander("🤖 View Agent Comments & Reasoning", expanded=False):
        if comments:
            st.markdown("**Agent Comments:**");
            for i, c in enumerate(comments): st.markdown(f"**Agent {i+1}:**"); st.text(c if c else "_No comment_")
            st.markdown("---")
        else: st.markdown("_No agents comments._")
        if reasoning_steps:
            st.markdown("**Generated Reasoning Steps:**"); st.markdown("\n".join([f"{i+1}. {s}" for i,s in enumerate(reasoning_steps)]))
        else: st.markdown("_No reasoning steps._")

def display_navigation_controls(total_items: int):
    current_index = st.session_state.current_qa_index; nav_cols = st.columns([1, 3, 1])
    with nav_cols[0]:
        if st.button("⬅️ Previous", use_container_width=True, disabled=(current_index <= 0)): st.session_state.current_qa_index -= 1; st.rerun()
    with nav_cols[1]:
        jump_to = st.number_input("Go to Q #", 1, total_items, current_index + 1, 1, key=f"jump_to_{current_index}", label_visibility="collapsed", help=f"Question (1-{total_items})")
        if jump_to != current_index + 1:
            new_index = jump_to - 1
            if 0 <= new_index < total_items: st.session_state.current_qa_index = new_index; st.rerun()
    with nav_cols[2]:
        btn_txt="Skip / Next ➡️" if current_index<total_items-1 else "End"; btn_dis=current_index>=total_items-1
        if st.button(btn_txt, use_container_width=True, disabled=btn_dis):
             if not btn_dis: st.session_state.current_qa_index += 1; st.rerun()

def display_progress_summary(total_items: int):
    if not st.session_state.annotated or len(st.session_state.annotated) != total_items: st.session_state.annotated = [None] * total_items
    decisions = {"correct": 0, "modified": 0, "modified_question": 0, "rejected": 0}; annotated_count = 0
    for item in st.session_state.annotated:
        if item is not None:
            decision = item.get("human_decision");
            if decision in decisions: decisions[decision] += 1; annotated_count += 1
    skipped_count = total_items - annotated_count; completed = annotated_count
    st.progress(completed / total_items if total_items > 0 else 0)
    st.caption(f"Completed: {completed}/{total_items} ({int(completed/total_items*100) if total_items > 0 else 0}%)")
    col1, col2 = st.columns(2)
    with col1: st.metric("✅ Correct", decisions["correct"]); st.metric("🖊️ Mod Answer", decisions["modified"]); st.metric("❓ Skipped", skipped_count)
    with col2: st.metric("📝 Mod Q&A", decisions["modified_question"]); st.metric("❌ Rejected", decisions["rejected"])

# --- Main Application Logic ---
def main():
    st.markdown("<h1 class='main-header'>📝 Human Annotation Tool</h1>", unsafe_allow_html=True)
    # --- State Init ---
    if 'current_qa_index' not in st.session_state: st.session_state.current_qa_index = 0
    if 'qa_pairs' not in st.session_state: st.session_state.qa_pairs = []
    if 'annotated' not in st.session_state: st.session_state.annotated = []
    if 'tables_loaded' not in st.session_state: st.session_state.tables_loaded = False
    if 'pair_path' not in st.session_state: st.session_state.pair_path = None
    if 'annotator_name' not in st.session_state: st.session_state.annotator_name = ""
    if 'table1_data' not in st.session_state: st.session_state.table1_data = None
    if 'table2_data' not in st.session_state: st.session_state.table2_data = None
    if 'confirm_start_over' not in st.session_state: st.session_state.confirm_start_over = False

    # --- Sidebar ---
    with st.sidebar:
        # ... (Setup, Data Selection, Load Button logic - same as previous version) ...
        st.header("⚙️ Setup & Progress"); st.session_state.annotator_name = st.text_input("👤 Your Name:", st.session_state.annotator_name)
        annotator_provided = bool(st.session_state.annotator_name.strip());
        if not annotator_provided: st.warning("Please enter your name.")
        st.markdown("---"); st.subheader("📂 Select Data")
        root_folder = st.text_input("Root Folder Path:", "BIRD_INSTRUCT", disabled=not annotator_provided)
        pair_path_selected = None
        if annotator_provided and os.path.exists(root_folder):
            databases = find_folders(root_folder)
            if databases:
                selected_db_index = st.session_state.get('selected_db_index', 0)
                if selected_db_index >= len(databases): selected_db_index = 0
                selected_db = st.selectbox("Select Database:", databases, index=selected_db_index, key='db_select')
                if selected_db:
                    db_path = os.path.join(root_folder, selected_db); table_pairs = find_folders(db_path)
                    if table_pairs:
                        selected_pair_index = st.session_state.get('selected_pair_index', 0)
                        if selected_pair_index >= len(table_pairs): selected_pair_index = 0
                        selected_pair_folder = st.selectbox("Select Table Pair Folder:", table_pairs, index=selected_pair_index, key='pair_select')
                        if selected_pair_folder:
                            pair_path_selected = os.path.join(db_path, selected_pair_folder)
                            if st.button("Load Data", use_container_width=True):
                                with st.spinner("Loading data..."):
                                    st.session_state.current_qa_index=0; st.session_state.qa_pairs=[]; st.session_state.annotated=[]
                                    st.session_state.table1_data=None; st.session_state.table2_data=None; st.session_state.tables_loaded=False; st.session_state.pair_path=None
                                    t1f,t2f=find_subset_files(pair_path_selected); qf=find_qa_file(pair_path_selected)
                                    if t1f and t2f and qf:
                                        t1d=load_json_file(t1f); t2d=load_json_file(t2f); qd=load_json_file(qf)
                                        if t1d is not None and t2d is not None and qd is not None:
                                            st.session_state.table1_data=t1d; st.session_state.table2_data=t2d; st.session_state.pair_path=pair_path_selected
                                            ql=[];
                                            if isinstance(qd, list): ql=qd
                                            elif isinstance(qd, dict):
                                                for v in qd.values():
                                                    if isinstance(v, list): ql=v; break
                                            st.session_state.qa_pairs=ql
                                            if st.session_state.qa_pairs:
                                                st.session_state.annotated=[None]*len(ql); st.session_state.tables_loaded=True
                                                st.session_state.selected_db_index=databases.index(selected_db); st.session_state.selected_pair_index=table_pairs.index(selected_pair_folder)
                                                st.success(f"Loaded {len(ql)} QA pairs."); st.rerun()
                                            else: st.warning("No QA pairs found.")
                                        else: st.error("Failed loading data from files.")
                                    else: st.error("Required files not found.")
        # --- Progress, Saving, Reset ---
        if st.session_state.tables_loaded and st.session_state.qa_pairs:
             st.markdown("---"); st.subheader("📊 Annotation Progress"); display_progress_summary(len(st.session_state.qa_pairs))
             st.markdown("---"); st.subheader("💾 Save Annotations")
             annotated_items=[i for i in st.session_state.annotated if i is not None]; save_disabled=not annotated_items
             clean_name="".join(c for c in st.session_state.annotator_name if c.isalnum() or c in "_-").lower()
             annotator_fp=os.path.join(st.session_state.pair_path, f"{clean_name}_annotations.json")
             if st.button(f"Save as {clean_name}_annotations.json", key="annotator_save", use_container_width=True, disabled=save_disabled):
                  if save_json_file(annotated_items, annotator_fp): st.toast("Saved!", icon="✅")
                  else: st.toast("Save failed!", icon="❌")
             final_fp = os.path.join(st.session_state.pair_path, "final_annotations.json")
             if st.button("Save as final_annotations.json", key="final_save", use_container_width=True, type="primary", disabled=save_disabled):
                  if save_json_file(annotated_items, final_fp): st.toast("Saved final!", icon="✅")
                  else: st.toast("Save failed!", icon="❌")
             st.markdown("---"); st.subheader("🔄 Reset")
             st.session_state.confirm_start_over = st.checkbox("Confirm Start Over", value=st.session_state.confirm_start_over)
             if st.button("Start Over", use_container_width=True, disabled=not st.session_state.confirm_start_over, key="reset_button"):
                 st.session_state.current_qa_index=0; st.session_state.annotated=[None]*len(st.session_state.qa_pairs); st.session_state.confirm_start_over=False
                 st.toast("Progress reset.", icon="🔄"); time.sleep(1); st.rerun()

    # --- Main Content Area ---
    if st.session_state.tables_loaded and st.session_state.qa_pairs:
        current_index = st.session_state.current_qa_index
        total_pairs = len(st.session_state.qa_pairs)
        # Header
        st.markdown(f"""<div class='annotator-info'>Annotating as: <strong>{st.session_state.annotator_name}</strong> | Folder: <strong>{os.path.basename(st.session_state.pair_path)}</strong> ({os.path.basename(os.path.dirname(st.session_state.pair_path))})</div>""", unsafe_allow_html=True)

        if 0 <= current_index < total_pairs:
            qa_pair = st.session_state.qa_pairs[current_index]
            main_col1, main_col2 = st.columns([2, 1]) # Ratio: Tables+Actions | Details

            # --- Get Original Data ---
            original_question = qa_pair.get("question", "N/A")
            # Get the original answer dictionary { "data": [...] }
            original_answer_dict = qa_pair.get("answer", {"data": []}) # Default to valid structure
            # Create the JSON string representation for display/editing
            original_answer_json_string = answer_dict_to_json_string(original_answer_dict)

            # --- Left Column: Tables & Actions ---
            with main_col1:
                st.markdown("<h3 class='sub-header'>Reference Tables</h3>", unsafe_allow_html=True)
                table_col1, table_col2 = st.columns(2)
                with table_col1: display_table(st.session_state.table1_data, "Table 1")
                with table_col2: display_table(st.session_state.table2_data, "Table 2")
                # --- Annotation Actions ---
                st.markdown("---"); st.markdown("##### 👇 Choose Annotation Action", unsafe_allow_html=True)
                action_cols = st.columns(4)
                # Get potentially modified text from widgets in main_col2
                modified_answer_json_text = st.session_state.get(f"mod_ans_{current_index}", original_answer_json_string)
                modified_question_text = st.session_state.get(f"mod_qn_{current_index}", original_question)

                # --- Action Button Logic ---
                base_annotation = { # Store original structure
                    "id": qa_pair.get("id", f"item_{current_index}"), "original_question": original_question,
                    "original_answer_dict": original_answer_dict, # Store original dict
                    "original_reasoning_steps": qa_pair.get("reasoning_steps",[]), "original_tables_used": qa_pair.get("tables_used",[]),
                    "original_question_type": qa_pair.get("question_type",""), "verification": qa_pair.get("verification",{}),
                    "annotator_name": st.session_state.annotator_name, "annotation_timestamp": pd.Timestamp.now().isoformat(),
                }
                def submit_annotation(annotation_update):
                    st.session_state.annotated[current_index] = {**base_annotation, **annotation_update}
                    # Remove original_answer_dict before saving final annotation if desired
                    if "original_answer_dict" in st.session_state.annotated[current_index]:
                         del st.session_state.annotated[current_index]["original_answer_dict"]
                    st.session_state.current_qa_index += 1; st.rerun()

                with action_cols[0]:
                    if st.button("✅ Correct", use_container_width=True):
                        update = {"question": original_question, "answer": original_answer_dict, # Save original dict
                                  "annotation_source": "human_verified_correct", "human_decision": "correct"}
                        st.toast("Correct!", icon="✅"); submit_annotation(update)
                with action_cols[1]:
                    if st.button("🖊️ Mod Answer", use_container_width=True):
                        parsed_answer_dict, is_valid = json_string_to_answer_dict(modified_answer_json_text)
                        if not is_valid: st.error("Invalid JSON format in modified answer.")
                        else:
                            update = {"question": original_question, "answer": parsed_answer_dict, # Save parsed dict
                                      "annotation_source": "human_modified_answer", "human_decision": "modified"}
                            st.toast("Answer Modified!", icon="🖊️"); submit_annotation(update)
                with action_cols[2]:
                    if st.button("📝 Mod Q&A", use_container_width=True):
                        parsed_answer_dict, is_valid = json_string_to_answer_dict(modified_answer_json_text)
                        if not is_valid: st.error("Invalid JSON format in modified answer.")
                        else:
                            update = {"question": modified_question_text, "answer": parsed_answer_dict, # Save parsed dict
                                      "annotation_source": "human_modified_question_answer", "human_decision": "modified_question"}
                            st.toast("Q&A Modified!", icon="📝"); submit_annotation(update)
                with action_cols[3]:
                    if st.button("❌ Reject", use_container_width=True):
                        update = {"question": original_question, "answer": original_answer_dict, # Save original dict
                                  "annotation_source": "human_rejected", "human_decision": "rejected"}
                        st.toast("Rejected!", icon="❌"); submit_annotation(update)

            # --- Right Column: Annotation Details ---
            with main_col2:
                st.markdown(f"<h3 class='sub-header'>Details (Q {current_index + 1}/{total_pairs})</h3>", unsafe_allow_html=True)
                # Agent Status
                verification = qa_pair.get("verification",{}); is_selected = verification.get("is_selected",False)
                st.markdown(f"<div class='verification-box {'verified-true' if is_selected else 'verified-false'}'>{'✅ Agent Consensus: SELECTED' if is_selected else '❌ Agent Consensus: NOT Selected'}</div>", unsafe_allow_html=True)
                # Question
                st.markdown("<div class='box question-box'>", unsafe_allow_html=True)
                st.markdown("<strong>❓ Original Question:</strong>", unsafe_allow_html=True); st.info(original_question)
                c1,c2=st.columns(2);
                with c1: qt=qa_pair.get("question_type",""); qts=f"`{qt}`" if qt else ""; st.write(f"**Type:** {qts}")
                with c2: tu=qa_pair.get("tables_used",[]); tus=f"`{', '.join(tu)}`" if tu else ""; st.write(f"**Tables Used:** {tus}")
                st.markdown("</div>", unsafe_allow_html=True)
                # Answer (Display JSON string)
                st.markdown("<div class='box answer-box'>", unsafe_allow_html=True)
                st.markdown("<strong>💡 Original Answer (JSON):</strong>", unsafe_allow_html=True)
                # Use st.code for better JSON formatting display
                st.code(original_answer_json_string, language="json", line_numbers=False)
                st.markdown("</div>", unsafe_allow_html=True)

                # Modification Areas (Widgets)
                st.markdown("<strong>✏️ Verify / Modify Answer (JSON):</strong>", unsafe_allow_html=True)
                # Use JSON string as default value
                st.text_area("Correct answer (must be valid JSON like {\"data\": [ ... ]}):",
                             value=original_answer_json_string, # Default is JSON string
                             height=180, # Taller for JSON structure
                             label_visibility="collapsed",
                             key=f"mod_ans_{current_index}",
                             help="Edit the JSON directly. Ensure it remains valid.")
                with st.expander("📝 Edit Question (Optional)", expanded=False):
                    st.text_area("Correct question:", value=original_question, height=80, key=f"mod_qn_{current_index}")

                # Agent Details & Nav
                st.markdown("---"); display_agent_votes_summary(verification); display_agent_comments_reasoning(qa_pair)
                st.markdown("---"); display_navigation_controls(total_pairs)

        else: # End of annotations
            st.success("🎉 All QA pairs reviewed!")
            st.markdown("---"); st.markdown("Use sidebar to save/reset. Use navigation controls to revisit.")
    # Initial State Messages
    elif not st.session_state.annotator_name: st.info("👋 Welcome! Please enter name in sidebar.")
    elif not st.session_state.tables_loaded: st.info("📂 Please select data and click 'Load Data' in sidebar.")

if __name__ == "__main__":
    main()