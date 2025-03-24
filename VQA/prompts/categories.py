def load_question_types_and_examples():
    types = [
        "Match-Based Fact Checking",
        "Multi-hop Fact Checking",
        "Arithmetic Calculation",
        "SELECT",
        "LIST",
        "COUNT",
        "Comparison",
        "Aggregation",
        "Ranking",
        "Counting",
        "Time-based Calculation",
        "Multi-hop Numerical Reasoning",
        "Descriptive Analysis",
        "Anomaly Detection",
        "Statistical Analysis",
        "Correlation Analysis"
    ]
    
    few_shot_examples = {
        # Fact Checking Tasks
        "Match-Based Fact Checking": [
            "Does the table indicate that Company A had an operating expense of $2.25 in 2018?",
            "Is it true that the fuel expense for 2017 was $1.74 based on the table?"
        ],
        "Multi-hop Fact Checking": [
            "Based on the table, if the operating expense in 2018 is greater than in 2017 and the fuel expense is higher, can we conclude that fuel consumption was also higher?",
            "Check if the table supports the claim that higher fuel consumption in 2017 led to increased operating expenses."
        ],
        # Numerical Reasoning Tasks
        "Arithmetic Calculation": [
            "Calculate the sum of operating expenses for the years 2016, 2017, and 2018.",
            "What is the difference between the operating expense in 2018 and 2016?"
        ],
        "Comparison": [
            "Which year had a higher operating expense: 2017 or 2018?",
            "Compare the fuel expense for 2017 and 2018."
        ],
        "Aggregation": [
            "What is the average operating expense over the three years?",
            "Find the total operating expense across all years in the table."
        ],
        "Ranking": [
            "Rank the years based on the operating expenses from highest to lowest.",
            "Order the years based on fuel consumption."
        ],
        "Counting": [
            "How many years in the table show an operating expense above $6000?",
            "Count the number of entries where the fuel expense is less than $2.00."
        ],
        "Time-based Calculation": [
            "What is the percentage increase in operating expense from 2017 to 2018?",
            "Calculate the growth rate of operating expense from 2016 to 2018."
        ],
        "Multi-hop Numerical Reasoning": [
            "Determine the total fuel expense for 2017 and 2018, and then compare it to the total operating expense in those years.",
            "Compute the difference in operating expense between 2018 and 2017, then divide by the operating expense in 2017 to get the rate of change."
        ],
        # Data Analysis Tasks
        "Descriptive Analysis": [
            "Describe the overall trend in operating expenses over the years.",
            "Summarize the distribution of fuel expenses across the given years."
        ],
        "Anomaly Detection": [
            "Identify any outliers in the operating expenses from 2016 to 2018.",
            "Are there any unusual values in the fuel expense column?"
        ],
        "Statistical Analysis": [
            "Calculate the standard deviation of the operating expenses in the table.",
            "What is the variance in fuel expenses over the years?"
        ],
        "Correlation Analysis": [
            "Is there a correlation between operating expenses and fuel expenses?",
            "Analyze the relationship between fuel consumption and operating expenses."
        ],
        "Causal Analysis": [
            "Does an increase in fuel consumption appear to cause an increase in operating expenses?",
            "Examine if a change in fuel expense affects the operating expense."
        ],
        # SQL Tasks
        "SELECT": [
            "Select the operating expenses for the year 2018.",
            "Show the fuel expenses for the year 2017.",
            "What are the ids of the courses that are registered or attended by the student whose id is 121?"
            
        ],
        "LIST": [
            "List the years with operating expenses above $5000.",
            "Show the years with fuel expenses less than $2.00.",
            "Find the cell mobile number of the candidates whose assessment code is Fail?"

        ],
        "COUNT": [
            "Count the number of years with operating expenses above $6000.",
            "How many years have fuel expenses less than $2.00?",
        ],
    }
    
    return types, few_shot_examples


