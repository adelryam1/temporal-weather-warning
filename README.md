# Automated Temporal Weather Warning System

This project is an automated weather monitoring and alert system built using Python and Temporal. It fetches real-time precipitation data for specific cities, analyzes the flood risk, and utilizes **Gemini AI** to generate a formatted social media advisory.

## 📁 File Structure
* `weather_workflow.py`: Contains the Activity definitions (API fetching, risk analysis, post generation) and the Workflow definition. It includes a bypass for Temporal's strict sandbox to safely allow the `requests` library in activities.
* `main.py`: The execution engine. It connects to the Temporal server, registers the Worker, schedules the workflow to run automatically every hour (`@hourly`), and triggers an immediate test run.

## 🛠️ Prerequisites
Before running the system, ensure you have the following installed:
1. **Python 3.x**
2. **Temporal CLI** (Local development server)
3. Required Python libraries:
   ```bash
   pip install temporalio requests google-genai

### 🔑 Configuration
This project uses Google's Gemini AI to generate the social media posts. 
Before running the system, you must provide your own API key:
1. Get a free API key from Google AI Studio.
2. Open `weather_workflow.py`.
3. Locate `api_key="YOUR_API_KEY_HERE"` and replace the placeholder with your actual key.

## 🚀 How to Run
Step 1: Start the Temporal Server
Open a terminal and start your local Temporal development server:
    Bash
    temporal server start-dev

Step 2: Run the Worker and Engine
Open a separate terminal in the project directory and run the main script:
    Bash    
    python main.py

Step 3: View the Output
The terminal will immediately output a test run, displaying the live fetched data, the risk analysis, and the final social media post.

The worker will then remain running in the background to fulfill the @hourly cron schedule.

You can view the scheduled and completed workflows by navigating to the Temporal Web UI at http://localhost:8233.

To stop the background worker, simply press Ctrl+C in the terminal running main.py.