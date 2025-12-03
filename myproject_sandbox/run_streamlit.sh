#!/bin/bash

# Add timestamp for logging
echo "============================" >> /home/ubuntu/myproject/streamlit.log
echo "Streamlit run started at: $(date)" >> /home/ubuntu/myproject/streamlit.log

# Navigate to your project directory
cd /home/ubuntu/myproject1/myproject

# Run API python file and log the output
#nohup /home/ubuntu/.venv/bin/streamlit myproject.py --server.port=8501 --server.address=127.0.0.1 >> /home/ubuntu/myproject/streamlit.log 2>&1 &
source ~/.venv/bin/activate
nohup /home/ubuntu/.venv/bin/python -m uvicorn myproject:app --reload >> /home/ubuntu/myproject1/myproject/streamlit.log 2>&1 &
