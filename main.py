import asyncio
import concurrent.futures  # <--- NEW IMPORT ADDED HERE
from temporalio.client import Client
from temporalio.worker import Worker

# Import our building blocks from your blueprint file!
from weather_workflow import (
    WeatherWarningWorkflow, 
    fetch_weather_data, 
    analyze_weather_risks, # <--- UPDATED THIS NAME
    generate_warning_post
)

async def main():
    # 1. Connect to the local Temporal server
    client = await Client.connect("localhost:7233")

    # 2. Create a Worker (the factory that executes your code)
    worker = Worker(
        client,
        task_queue="weather-task-queue",
        workflows=[WeatherWarningWorkflow],
        # V--- UPDATED THE NAME IN THIS LIST TOO
        activities=[fetch_weather_data, analyze_weather_risks, generate_warning_post],
        activity_executor=concurrent.futures.ThreadPoolExecutor(), # <--- NEW LINE ADDED HERE
    )

    # 3. Schedule the workflow to run every hour
    try:
        await client.start_workflow(
            WeatherWarningWorkflow.run,
            id="hourly-weather-workflow",
            task_queue="weather-task-queue",
            cron_schedule="@hourly", 
        )
        print("🕒 Hourly weather workflow successfully scheduled!")
    except Exception:
        print("🕒 Hourly workflow is already scheduled.")

    # 4. For testing, let's also force it to run right now so we can see the result!
    print("🚀 Triggering an immediate test run...\n")
    
    # Start the worker in the background
    worker_task = asyncio.create_task(worker.run())
    
    # Execute a manual run immediately
    result = await client.execute_workflow(
        WeatherWarningWorkflow.run,
        id="immediate-weather-test",
        task_queue="weather-task-queue",
    )
    
    print("\n=========================================")
    print(f"📢 FINAL SOCIAL MEDIA POST:\n{result}")
    print("=========================================\n")
    print("⏳ The worker is now running in the background for the hourly tasks. Press Ctrl+C to quit.")
    
    await worker_task

if __name__ == "__main__":
    asyncio.run(main())