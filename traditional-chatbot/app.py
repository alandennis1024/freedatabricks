import gradio as gr
from typing import List
# Import necessary libraries

# Define the chatbot response function with type hints and docstring
def chatbot_response(message: str, history: List[List[str]]) -> str:
    """
    Generate a rule-based response to the user's message.
    Parameters:
        message (str): The user's input message.
        history (List[List[str]]): The conversation history (not used in this implementation except for 'history' keyword).
    Returns:
        str: The chatbot's response based on keyword matching.
    """
    message = message.lower()
    result ="I'm not sure how to respond to that. Can you please rephrase?"
    if "history" in message:
        history_str = "\n".join([f"User: {h[0]} | Bot: {h[1]}" for h in history])
        result =  f"History Length:{len(history)}, Conversation history:\n{history_str}" if history else "No conversation history yet."
    elif "hello" in message or "hi" in message:
        result= "Hello! How can I help you today?"
    elif "help" in message:
        result= "Sure, I'm here to help. What do you need assistance with?"
    elif "bye" in message or "goodbye" in message:
        result= "Goodbye! Have a great day!"
    return result

iface = gr.ChatInterface(
  fn=chatbot_response,
  title="Rule-Based Chatbot",
  description="A simple rule-based chatbot built with Gradio."
)

if __name__ == "__main__":
    iface.launch()  # Launch the Gradio interface with sharing enabled
