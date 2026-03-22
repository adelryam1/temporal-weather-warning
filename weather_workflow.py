from temporalio import activity, workflow
from datetime import timedelta

# This tells Temporal's security guard to ignore the requests library!
with workflow.unsafe.imports_passed_through():
    import requests

# These are the 3 cities we are checking (Manila, Cebu, Iligan)
# We use their Latitude and Longitude for the weather API
CITIES = {
    "Manila": {"lat": 14.5995, "lon": 120.9842},
    "Cebu": {"lat": 10.3157, "lon": 123.8854},
    "Iligan": {"lat": 8.2280, "lon": 124.2452}
}

@activity.defn
def fetch_weather_data() -> dict:
    """
    Activity 1: Fetches current weather data for our 3 cities.
    """
    print("🌤️ Fetching weather data from Open-Meteo API...")
    results = {}
    
    for city, coords in CITIES.items():
        # We are asking the API for the current precipitation (rain) amount
        url = f"https://api.open-meteo.com/v1/forecast?latitude={coords['lat']}&longitude={coords['lon']}&current=precipitation"
        
        response = requests.get(url)
        data = response.json()
        
        # Save the rain amount (in mm) for the city
        rain_amount = data["current"]["precipitation"]
        results[city] = rain_amount
        print(f"   -> {city}: {rain_amount}mm of rain")
        
    return results

@activity.defn
def analyze_flood_risk(weather_data: dict) -> dict:
    """
    Activity 2: Determines flood risk based on the amount of rain.
    """
    print("🔍 Analyzing flood risk for each city...")
    risk_report = {}
    
    for city, rain in weather_data.items():
        # Simple risk logic based on mm of rain
        if rain >= 50.0:
            risk = "HIGH RISK 🚨 (Severe Flooding Possible)"
        elif rain >= 15.0:
            risk = "MODERATE RISK ⚠️ (Minor Flooding Possible)"
        else:
            risk = "LOW RISK ✅ (Safe)"
            
        risk_report[city] = {"rain_mm": rain, "risk_level": risk}
        print(f"   -> {city}: {risk}")
        
    return risk_report

@activity.defn
def generate_warning_post(risk_report: dict) -> str:
    """
    Activity 3: Drafts a social media warning for high-risk areas.
    """
    print("📱 Drafting social media warnings...")
    
    # Filter for only the cities in danger
    cities_in_danger = []
    for city, data in risk_report.items():
        if "HIGH" in data["risk_level"] or "MODERATE" in data["risk_level"]:
            cities_in_danger.append(f"{city} ({data['rain_mm']}mm rain)")
            
    if not cities_in_danger:
        post = "Weather Update: All monitored cities are currently experiencing safe weather conditions. Stay dry! ☂️"
        print(f"   -> {post}")
        return post
        
    danger_list = ", ".join(cities_in_danger)
    post = f"🚨 WEATHER ADVISORY: Elevated flood risks detected in the following areas: {danger_list}. Please take precautions and stay tuned to local authorities! 🚨"
    print(f"   -> {post}")
    
    return post

@workflow.defn
class WeatherWarningWorkflow:
    @workflow.run
    async def run(self) -> str:
        print("🚀 Starting Weather Warning Workflow...")
        
        # Step 1: Run the fetch activity
        weather_data = await workflow.execute_activity(
            fetch_weather_data,
            start_to_close_timeout=timedelta(seconds=15)
        )
        
        # Step 2: Pass the weather data to the risk analysis activity
        risk_report = await workflow.execute_activity(
            analyze_flood_risk,
            weather_data,
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        # Step 3: Pass the risk report to the social media activity
        final_post = await workflow.execute_activity(
            generate_warning_post,
            risk_report,
            start_to_close_timeout=timedelta(seconds=10)
        )
        
        print("✅ Workflow complete!")
        return final_post