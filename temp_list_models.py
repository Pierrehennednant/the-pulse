import google.generativeai as genai
import os

genai.configure(api_key="AIzaSyBc6qEIbELpXhWP5-ldrTb4INoU3ZI7I4k")

for model in genai.list_models():
    if 'generateContent' in model.supported_generation_methods:
        print(model.name)
