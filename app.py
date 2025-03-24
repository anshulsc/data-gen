import streamlit as st
import os
import json
import pandas as pd
from typing import List, Dict, Any, Tuple

st.set_page_config(
    page_title="Human Answer Annotation Tool",
    page_icon="📝",
    layout="wide"
)


st.markdown("""
<style>
    .main-header {text-align: center; margin-bottom: 10px; font-size: 1.8rem;}
    .question-box {background-color: #f0f2f6; padding: 10px; border-radius: 5px; margin-bottom: -5px;}
    .answer-box {background-color: #e8f0fe; padding: 10px; border-radius: 5px; margin-bottom: 10px;}
    .stButton>button {width: 100%;}
    .annotation-section {border: 1px solid #ddd; border-radius: 5px; padding: 15px; margin-bottom: 15px;}
    .stProgress > div > div > div {height: 10px;}
    .verification-box {padding: 10px; border-radius: 5px; margin-bottom: 10px;}
    .verified-true {background-color: #d4edda; border: 1px solid #c3e6cb;}
    .verified-false {background-color: #f8d7da; border: 1px solid #f5c6cb;}
    .sticky-top {position: sticky; top: 0; z-index: 999; background-color: white; padding: 5px 0;}
    .agent-info {border-left: 1px solid #eee; padding-left: 10px;}
    .table-container {margin-bottom: 10px; border: 1px solid #ddd; border-radius: 5px; padding: 5px;}
</style>
""", unsafe_allow_html=True)

def load_json_file(filepath: str) -> Dict:
    try:
        with open(filepath, 'r', encoding='utf-8') as f:
            return json.load(f)
    except Exception as e:
        st.error(f"Error loading file {filepath}: {str(e)}")
        return {}

def save_json_file(data: Dict, filepath: str) -> bool:

    try:
        with open(filepath, 'w', encoding='utf-8') as f:
            json.dump(data, f, indent=2, ensure_ascii=False)
        return True
    except Exception as e:
        st.error(f"Error saving file {filepath}: {str(e)}")
        return False

def find_folders(directory: str) -> List[str]:
    if not os.path.exists(directory):
        return []
    return [d for d in os.listdir(directory) if os.path.isdir(os.path.join(directory, d))]

def find_subset_files(directory: str) -> Tuple[str, str]:
    table1_file = None
    table2_file = None
    
    for file in os.listdir(directory):
        if file.endswith("_subset.json"):
            if table1_file is None:
                table1_file = os.path.join(directory, file)
            else:
                table2_file = os.path.join(directory, file)
                break
    
    return table1_file, table2_file

def find_qa_file(directory: str) -> str:
    qa_file = None
    
    # Look for files containing qa_pairs
    for file in os.listdir(directory):
        if "all_qa_pairs" in file and file.endswith(".json"):
            qa_file = os.path.join(directory, file)
            break
    
    return qa_file

def display_table(table_data: Dict) -> None:

    if not table_data:
        st.warning("No table data available")
        return
    
    st.markdown(f"**Table: {table_data.get('table_name', 'Unknown')}**")
    
    
    columns = table_data.get('columns', [])
    data = table_data.get('data', [])
    
    if columns and data:
        df = pd.DataFrame(data, columns=columns)
        st.dataframe(df, use_container_width=True, height=200)
    else:
        st.warning("Table data is incomplete")

def display_agent_votes(verification: Dict):

    valid_votes = verification.get("valid_votes", 0)
    multiple_tables_votes = verification.get("multiple_tables_votes", 0)
    total_votes = verification.get("total_votes", 0)
    avg_score = verification.get("average_score", 0)
    

    agent_data = []
    
    comments = verification.get("verification_comments", [])
    
    for i in range(min(3, total_votes)):
        agent_row = {
            "Agent": f"Agent {i+1}",
            "Vote": "Selected" if i < valid_votes else "Rejected",
            "Multi-Table": "Yes" if i < multiple_tables_votes else "No"
        }
        
        if i < len(comments):
            agent_row["Comment"] = comments[i]
        else:
            agent_row["Comment"] = "No comment provided"
            
        agent_data.append(agent_row)
    
    metrics_col1, metrics_col2, metrics_col3 = st.columns(3)
    with metrics_col1:
        st.metric("Valid Votes", f"{valid_votes}/{total_votes}")
    with metrics_col2:
        st.metric("Multi-Table Votes", multiple_tables_votes)
    with metrics_col3:
        st.metric("Average Score", f"{avg_score:.1f}/10")
    

    if agent_data:
        with st.expander("Agent Comments"):
            for i, comment in enumerate(comments):
                st.markdown(f"**Agent {i+1}:**")
                st.text(comment)
    else:
        st.info("No agent vote information available")

def main():
    st.markdown("<h1 class='main-header'>Human Answer Annotation Tool</h1>", unsafe_allow_html=True)
    
    if 'current_qa_index' not in st.session_state:
        st.session_state.current_qa_index = 0
    if 'qa_pairs' not in st.session_state:
        st.session_state.qa_pairs = []
    if 'annotated' not in st.session_state:
        st.session_state.annotated = []
    if 'tables_loaded' not in st.session_state:
        st.session_state.tables_loaded = False
    if 'pair_path' not in st.session_state:
        st.session_state.pair_path = None
    if 'annotator_name' not in st.session_state:
        st.session_state.annotator_name = ""
    
    if st.session_state.annotator_name:
        st.markdown(f"<div style='text-align: center; margin-bottom: 10px;'><strong>Current annotator:</strong> {st.session_state.annotator_name}</div>", unsafe_allow_html=True)
    
    with st.sidebar:
        st.header("Data Selection")
        st.session_state.annotator_name = st.text_input("Annotator Name:", st.session_state.annotator_name)
        
        root_folder = st.text_input("Enter the root folder path:", "BIRD_INSTRUCT")
        
        if not os.path.exists(root_folder):
            st.error(f"Folder '{root_folder}' does not exist")
            return
        
        databases = find_folders(root_folder)
        
        if not databases:
            st.warning(f"No databases found in '{root_folder}'")
            return
        
        selected_db = st.selectbox("Select a database:", databases)
        
        if not selected_db:
            return
        
        db_path = os.path.join(root_folder, selected_db)
        
        table_pairs = find_folders(db_path)
        
        if not table_pairs:
            st.warning(f"No table pairs found in '{db_path}'")
            return
        
        selected_pair = st.selectbox("Select a table pair:", table_pairs)
        
        if not selected_pair:
            return
        
        pair_path = os.path.join(db_path, selected_pair)
        st.session_state.pair_path = pair_path
        
        if st.button("Load Data", use_container_width=True):
            table1_file, table2_file = find_subset_files(pair_path)
            qa_file = find_qa_file(pair_path)
            
            if not table1_file or not table2_file:
                st.error(f"Could not find both table subset files in '{pair_path}'")
                return
            
            if not qa_file:
                st.error(f"Could not find QA pairs file in '{pair_path}'")
                return
            
            table1_data = load_json_file(table1_file)
            table2_data = load_json_file(table2_file)
            
            qa_data = load_json_file(qa_file)
            
            if not qa_data:
                st.error("Failed to load QA pairs data")
                return
            
            st.session_state.tables_loaded = True
            st.session_state.table1_data = table1_data
            st.session_state.table2_data = table2_data
            
            if isinstance(qa_data, list):
                st.session_state.qa_pairs = qa_data
            else:
                for key, value in qa_data.items():
                    if isinstance(value, list):
                        st.session_state.qa_pairs = value
                        break
            
            if not st.session_state.qa_pairs:
                st.error("No QA pairs found in the loaded data")
                return
            
            if not st.session_state.annotated:
                st.session_state.annotated = [None] * len(st.session_state.qa_pairs)
            
            st.success(f"Loaded {len(st.session_state.qa_pairs)} QA pairs")
            st.rerun()
        
        if st.session_state.tables_loaded and st.session_state.annotated:
            st.header("Annotation Progress")
            
            if st.session_state.annotator_name:
                st.markdown(f"**Annotator:** {st.session_state.annotator_name}")
            
            decisions = {
                "correct": 0,
                "modified": 0,
                "modified_question": 0,
                "rejected": 0,
                "skipped": 0
            }
            
            for item in st.session_state.annotated:
                if item is None:
                    decisions["skipped"] += 1
                else:
                    decision = item.get("human_decision", "skipped")
                    decisions[decision] = decisions.get(decision, 0) + 1
            
            total = len(st.session_state.qa_pairs)
            completed = decisions["correct"] + decisions["modified"] + decisions["modified_question"] + decisions["rejected"]
            
            st.progress(completed / total)
            st.write(f"Completed: {completed}/{total} ({int(completed/total*100)}%)")
            
            col1, col2 = st.columns(2)
            with col1:
                st.metric("Correct", decisions["correct"])
                st.metric("Modified Answer", decisions["modified"])
            with col2:
                st.metric("Modified Question", decisions.get("modified_question", 0))
                st.metric("Rejected", decisions["rejected"])
            
            st.metric("Skipped", decisions["skipped"])
            
            if completed > 0:
                save_col1, save_col2 = st.columns(2)
                
                with save_col1:
                    output_path = os.path.join(pair_path, "human_annotated.json")
                    if st.button("Save Annotations", use_container_width=True):
                        final_annotations = [item for item in st.session_state.annotated if item is not None]
                        
                        if save_json_file(final_annotations, output_path):
                            st.success(f"Annotations saved to {output_path}")
                        else:
                            st.error("Failed to save annotations")
                
                with save_col2:
                    final_output_path = os.path.join(pair_path, "final.json")
                    if st.button("Save as Final", use_container_width=True):
                        final_annotations = [item for item in st.session_state.annotated if item is not None]
                        
                        if save_json_file(final_annotations, final_output_path):
                            st.success(f"Final annotations saved to {final_output_path}")
                        else:
                            st.error("Failed to save final annotations")
    
                if st.session_state.annotator_name:
                    if st.button(f"Save as {st.session_state.annotator_name}_final.json", key="sidebar_annotator_save", use_container_width=True):
                        
                        clean_name = "".join(c for c in st.session_state.annotator_name if c.isalnum() or c in "_-").lower()
                        annotator_final_path = os.path.join(pair_path, f"{clean_name}_final.json")
                        final_annotations = [item for item in st.session_state.annotated if item is not None]
                        
                        if save_json_file(final_annotations, annotator_final_path):
                            st.success(f"Annotations saved to {annotator_final_path}")
                        else:
                            st.error("Failed to save annotations")
         
            if st.button("Start Over", use_container_width=True):
                st.session_state.current_qa_index = 0
                st.session_state.annotated = [None] * len(st.session_state.qa_pairs)
                st.rerun()
    
    # Main content area
    if st.session_state.tables_loaded:
 
        with st.container():
            st.markdown("<div class='sticky-top'>", unsafe_allow_html=True)
            
            table_cols = st.columns(2)
            
            with table_cols[0]:
                st.markdown("<div class='table-container'>", unsafe_allow_html=True)
                display_table(st.session_state.table1_data)
                st.markdown("</div>", unsafe_allow_html=True)
            
            with table_cols[1]:
                st.markdown("<div class='table-container'>", unsafe_allow_html=True)
                display_table(st.session_state.table2_data)
                st.markdown("</div>", unsafe_allow_html=True)
                
            st.markdown("</div>", unsafe_allow_html=True)
        

        qa_pairs = st.session_state.qa_pairs
        current_index = st.session_state.current_qa_index
        
        if current_index < len(qa_pairs):
            qa_pair = qa_pairs[current_index]
            

            annotator_provided = bool(st.session_state.annotator_name.strip())
            if not annotator_provided:
                st.warning("⚠️ Please enter your name in the sidebar before annotating.")
            

            main_content, agent_info = st.columns([3, 1])
            
            with main_content:

                st.markdown(f"<h3>Question {current_index + 1} of {len(qa_pairs)}</h3>", unsafe_allow_html=True)

                st.markdown("<div class='question-box'>", unsafe_allow_html=True)
                st.markdown("<strong>Original Question:</strong>", unsafe_allow_html=True)
                original_question = qa_pair.get("question", "No question available")
                st.info(original_question)
                

                question_type = qa_pair.get("question_type", "")
                if question_type:
                    st.write(f"**Type:** {question_type}")
                
     
                tables_used = qa_pair.get("tables_used", [])
                if tables_used:
                    st.write(f"**Tables Used:** {', '.join(tables_used)}")
                st.markdown("</div>", unsafe_allow_html=True)
                

                with st.expander("Edit Question"):
                    modified_question = st.text_area("Modified Question", value=original_question, height=80)
                    
                    if st.button("Apply Question Edit"):
        
                        st.session_state.current_modified_question = modified_question
                        st.success("Question modification will be applied when you submit your decision")
                
                # Original answer box
                st.markdown("<div class='answer-box'>", unsafe_allow_html=True)
                st.markdown("<strong>Original Answer:</strong>", unsafe_allow_html=True)
                answer_data = qa_pair.get("answer", {}).get("data", [])
                original_answer = answer_data[0][0] if answer_data and answer_data[0] else "No answer available"
                st.info(original_answer)
                st.markdown("</div>", unsafe_allow_html=True)
                
                st.markdown("<strong>Verify or Modify Answer:</strong>", unsafe_allow_html=True)
                modified_answer = st.text_area("", value=original_answer, height=100, label_visibility="collapsed")
                
                action_cols = st.columns([1, 1, 1, 1])
                
                with action_cols[0]:
                    if st.button("✓ Correct (No Change)", use_container_width=True, disabled=not annotator_provided):
                        st.session_state.annotated[current_index] = {
                            "id": qa_pair.get("id", ""),
                            "question": qa_pair.get("question", ""),
                            "answer": {
                                "data": [[original_answer]],
                                "annotation_source": "human verified"
                            },
                            "reasoning_steps": qa_pair.get("reasoning_steps", []),
                            "tables_used": qa_pair.get("tables_used", []),
                            "question_type": qa_pair.get("question_type", ""),
                            "verification": qa_pair.get("verification", {}),
                            "human_decision": "correct",
                            "annotator_name": st.session_state.annotator_name,
                            "annotation_timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        st.session_state.current_qa_index += 1
                        st.rerun()
                
                with action_cols[1]:
                    if st.button("🖊️ Submit Modified Answer", use_container_width=True, disabled=not annotator_provided):
                        st.session_state.annotated[current_index] = {
                            "id": qa_pair.get("id", ""),
                            "question": qa_pair.get("question", ""),
                            "answer": {
                                "data": [[modified_answer]],
                                "annotation_source": "human annotated"
                            },
                            "reasoning_steps": qa_pair.get("reasoning_steps", []),
                            "tables_used": qa_pair.get("tables_used", []),
                            "question_type": qa_pair.get("question_type", ""),
                            "verification": qa_pair.get("verification", {}),
                            "human_decision": "modified",
                            "annotator_name": st.session_state.annotator_name,
                            "annotation_timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        st.session_state.current_qa_index += 1
                        st.rerun()
                
                with action_cols[2]:
                    if st.button("📝 Submit Modified Question & Answer", use_container_width=True, disabled=not annotator_provided):
    
                        final_question = getattr(st.session_state, 'current_modified_question', original_question)
                        
                        st.session_state.annotated[current_index] = {
                            "id": qa_pair.get("id", ""),
                            "question": final_question,
                            "original_question": original_question,
                            "answer": {
                                "data": [[modified_answer]],
                                "annotation_source": "human annotated with modified question"
                            },
                            "reasoning_steps": qa_pair.get("reasoning_steps", []),
                            "tables_used": qa_pair.get("tables_used", []),
                            "question_type": qa_pair.get("question_type", ""),
                            "verification": qa_pair.get("verification", {}),
                            "human_decision": "modified_question",
                            "annotator_name": st.session_state.annotator_name,
                            "annotation_timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        st.session_state.current_qa_index += 1
                        st.rerun()
                
                with action_cols[3]:
                    if st.button("❌ Reject", use_container_width=True, disabled=not annotator_provided):
                        st.session_state.annotated[current_index] = {
                            "id": qa_pair.get("id", ""),
                            "question": qa_pair.get("question", ""),
                            "answer": {
                                "data": [[original_answer]],
                                "annotation_source": "human rejected"
                            },
                            "reasoning_steps": qa_pair.get("reasoning_steps", []),
                            "tables_used": qa_pair.get("tables_used", []),
                            "question_type": qa_pair.get("question_type", ""),
                            "verification": qa_pair.get("verification", {}),
                            "human_decision": "rejected",
                            "annotator_name": st.session_state.annotator_name,
                            "annotation_timestamp": pd.Timestamp.now().strftime("%Y-%m-%d %H:%M:%S")
                        }
                        st.session_state.current_qa_index += 1
                        st.rerun()

                nav_cols = st.columns(2)
                
                with nav_cols[0]:
                    if current_index > 0:
                        st.button("⬅️ Previous", use_container_width=True, on_click=lambda: setattr(st.session_state, 'current_qa_index', current_index - 1))
                
                with nav_cols[1]:
                    if current_index < len(qa_pairs) - 1:
                        st.button("Skip ➡️", use_container_width=True, on_click=lambda: setattr(st.session_state, 'current_qa_index', current_index + 1))
            
            with agent_info:
                st.markdown("<div class='agent-info'>", unsafe_allow_html=True)

                verification = qa_pair.get("verification", {})
                is_selected = verification.get("is_selected", False)
                
                st.markdown(
                    f"<div class='verification-box {'verified-true' if is_selected else 'verified-false'}'>"
                    f"<strong>{'✓ Selected by agents' if is_selected else '✗ NOT selected by agents'}</strong>"
                    f"</div>",
                    unsafe_allow_html=True
                )
                

                st.markdown("<h4>Agent Evaluation</h4>", unsafe_allow_html=True)
                display_agent_votes(verification)
                

                reasoning_steps = qa_pair.get("reasoning_steps", [])
                if reasoning_steps:
                    with st.expander("Reasoning Steps"):
                        for i, step in enumerate(reasoning_steps):
                            st.write(f"{i+1}. {step}")
                
                st.markdown("</div>", unsafe_allow_html=True)
                
        else:
            st.success("All QA pairs have been annotated!")
            
            
            if st.session_state.annotator_name:
                if st.button(f"Save as {st.session_state.annotator_name}_final.json", use_container_width=True):
                    clean_name = "".join(c for c in st.session_state.annotator_name if c.isalnum() or c in "_-").lower()
                    annotator_final_path = os.path.join(st.session_state.pair_path, f"{clean_name}_final.json")
                    final_annotations = [item for item in st.session_state.annotated if item is not None]
                    if save_json_file(final_annotations, annotator_final_path):
                        st.success(f"Annotations saved to {annotator_final_path}")
                    else:
                        st.error("Failed to save annotations")
            else:
                st.warning("Please enter an annotator name to save with personalized filename")
            
            st.button("Start New Batch", on_click=lambda: setattr(st.session_state, 'current_qa_index', 0))

if __name__ == "__main__":
    main()