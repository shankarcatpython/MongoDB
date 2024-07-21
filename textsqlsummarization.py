import os
import sqlite3
from transformers import T5ForConditionalGeneration, T5Tokenizer

# Suppress the symlink warning
os.environ["HF_HUB_DISABLE_SYMLINKS_WARNING"] = "1"

# Load the T5 model and tokenizer for both SQL generation and summarization
t5_model = T5ForConditionalGeneration.from_pretrained('t5-small')
t5_tokenizer = T5Tokenizer.from_pretrained('t5-small')

# Connect to the SQLite database
conn = sqlite3.connect('vehicles.db')
cursor = conn.cursor()

def generate_sql(natural_language_query):
    input_text = f"translate English to SQL: {natural_language_query}"
    input_ids = t5_tokenizer.encode(input_text, return_tensors="pt", max_length=512, truncation=True)
    output_ids = t5_model.generate(input_ids, max_length=150, num_beams=4, early_stopping=True)
    sql_query = t5_tokenizer.decode(output_ids[0], skip_special_tokens=True)
    return sql_query

def execute_sql(sql_query):
    try:
        cursor.execute(sql_query)
        return cursor.fetchall()
    except sqlite3.Error as e:
        print(f"An error occurred: {e.args[0]}")
        return []

def chunk_results_by_words(results, max_words=400):
    chunks = []
    current_chunk = []
    current_word_count = 0

    for row in results:
        row_text = ' '.join(map(str, row))
        word_count = len(row_text.split())
        
        if current_word_count + word_count > max_words:
            chunks.append(current_chunk)
            current_chunk = []
            current_word_count = 0
        
        current_chunk.append(row_text)
        current_word_count += word_count
    
    if current_chunk:
        chunks.append(current_chunk)

    return chunks

def summarize_text(text):
    input_text = f"summarize: {text}"
    input_ids = t5_tokenizer.encode(input_text, return_tensors="pt", max_length=512, truncation=True)
    summary_ids = t5_model.generate(input_ids, max_length=150, min_length=30, length_penalty=2.0, num_beams=4, early_stopping=True)
    summary = t5_tokenizer.decode(summary_ids[0], skip_special_tokens=True)
    return summary

def summarize_results(results, max_words=400):
    chunks = chunk_results_by_words(results, max_words)
    summaries = []
    for chunk in chunks:
        text_chunk = ' '.join(chunk)
        summaries.append(summarize_text(text_chunk))
    
    # Combine the individual chunk summaries into an overall summary
    overall_summary = summarize_text(' '.join(summaries))
    return overall_summary

# Example natural language query
nl_query = "Get the manufacturer and model of all vehicles priced under $10,000."

# Generate SQL query from natural language query
sql_query = generate_sql(nl_query)
print(f"Generated SQL Query: {sql_query}")

# Ensure SQL is valid and in English
if "Erhalten" in sql_query or not sql_query.lower().startswith("select"):
    print("Generated SQL query is invalid or not in English. Adjusting the query manually.")
    sql_query = "SELECT manufacturer, model FROM vehicles WHERE price < 10000"

# Execute the SQL query and fetch results
results = execute_sql(sql_query)
print(f"Number of results: {len(results)}")

# Summarize the results
if results:
    summary = summarize_results(results, max_words=400)
    print("Summary:")
    print(summary)

# Close the database connection
conn.close()
