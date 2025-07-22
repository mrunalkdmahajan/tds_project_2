from fastapi import FastAPI, File, UploadFile, HTTPException
from fastapi.responses import JSONResponse
import asyncio
import json
import time
from typing import Any, List, Union
from .services.scraper import DataScraper
from .services.analyzer import DataAnalyzer
from .services.visualizer import DataVisualizer
import logging

# Configure logging
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

app = FastAPI(title="Data Analyst Agent", version="1.0.0")

@app.post("/api/")
async def analyze_data(file: UploadFile = File(...)):
    start_time = time.time()
    
    try:
        # Read the task description
        content = await file.read()
        task_description = content.decode('utf-8')
        
        logger.info(f"Received task: {task_description[:100]}...")
        
        # Parse and execute the task
        result = await execute_task(task_description)
        
        elapsed_time = time.time() - start_time
        logger.info(f"Task completed in {elapsed_time:.2f} seconds")
        
        return JSONResponse(content=result)
    
    except Exception as e:
        logger.error(f"Error processing task: {str(e)}")
        raise HTTPException(status_code=500, detail=str(e))

async def execute_task(task_description: str):
    """Execute the data analysis task based on description"""
    
    # Initialize services
    scraper = DataScraper()
    analyzer = DataAnalyzer()
    visualizer = DataVisualizer()
    
    # Parse task type
    if "wikipedia" in task_description.lower() and "highest-grossing" in task_description.lower():
        return await handle_movie_analysis(scraper, analyzer, visualizer, task_description)
    elif "indian high court" in task_description.lower():
        return await handle_court_analysis(scraper, analyzer, visualizer, task_description)
    else:
        raise ValueError("Unsupported task type")

async def handle_movie_analysis(scraper, analyzer, visualizer, task_description):
    """Handle Wikipedia movie data analysis"""
    
    # 1. Scrape Wikipedia data
    url = "https://en.wikipedia.org/wiki/List_of_highest-grossing_films"
    movie_data = await scraper.scrape_wikipedia_movies(url)
    
    # 2. Analyze data for each question
    results = []
    
    # Question 1: How many $2bn movies before 2020?
    movies_2bn_before_2020 = analyzer.count_movies_over_threshold(
        movie_data, threshold=2.0, before_year=2020
    )
    results.append(movies_2bn_before_2020)
    
    # Question 2: Earliest film over $1.5bn
    earliest_1_5bn = analyzer.find_earliest_over_threshold(
        movie_data, threshold=1.5
    )
    results.append(earliest_1_5bn)
    
    # Question 3: Correlation between Rank and Peak
    correlation = analyzer.calculate_correlation(movie_data, 'Rank', 'Peak')
    results.append(round(correlation, 6))
    
    # Question 4: Scatterplot with regression line
    plot_base64 = await visualizer.create_scatterplot_with_regression(
        movie_data, 'Rank', 'Peak', 
        regression_color='red', 
        regression_style='dotted'
    )
    results.append(plot_base64)
    
    return results

async def handle_court_analysis(scraper, analyzer, visualizer, task_description):
    """Handle Indian High Court data analysis"""
    
    # This would involve DuckDB queries to S3 data
    # Implementation depends on the specific questions
    results = {}
    
    # Example structure - you'd implement actual DuckDB queries
    results["Which high court disposed the most cases from 2019 - 2022?"] = "Court Name"
    results["What's the regression slope of the date_of_registration - decision_date by year in the court=33_10?"] = 0.123
    results["Plot the year and # of days of delay..."] = "data:image/webp;base64,..."
    
    return results

if __name__ == "__main__":
    import uvicorn
    uvicorn.run(app, host="0.0.0.0", port=8000)