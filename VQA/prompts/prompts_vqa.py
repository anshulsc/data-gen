
VQA_PROMPT = """
    Generate a rich multi-hop question-answer pair related to the {table_count} tables below. The question MUST require reasoning across MULTIPLE tables.
    
    [Tables]
    {tables_json}
    
    [Question Type]
    {question_type}
    
    [Few-shot Examples]
    {examples}
    
    Instructions:
    1. Create ONE complex question that requires multi-step reasoning and joins across AT LEAST 2 of the provided tables
    2. The question should specifically be of type: {question_type} and should be extracted from multiple tables.
    3. Provide detailed reasoning steps to arrive at the answer
    4. Return ONLY a JSON object, with no markdown formatting, code blocks, or explanatory text
    5. Use this exact JSON format:
    
    {{
        "id": "{unique_id}",
        "question": "Your complex question here",
        "answer": {{
            "data": [[value1], [value2], ...],
        }},
        "reasoning_steps": [
            "Step 1: Describe what data/tables you're looking at",
            "Step 2: Explain the specific operations/joins needed",
            "Step 3: Show calculations or logic used",
            "Step 4: Arrive at the final answer"
        ],
        "tables_used": ["table1", "table2"],
        "question_type": "{question_type}"
    }}
    
    IMPORTANT: 
    - Your response must be a valid JSON object and nothing else
    - Your question MUST require joins and reasoning across at least 2 different tables
    
    
    """