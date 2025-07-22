import pandas as pd
import numpy as np
from scipy import stats
from typing import Any, Optional
import logging

logger = logging.getLogger(__name__)

class DataAnalyzer:
    
    def count_movies_over_threshold(self, df: pd.DataFrame, threshold: float, before_year: int) -> int:
        """Count movies over gross threshold before given year"""
        
        if 'Gross_Numeric' not in df.columns or 'Year' not in df.columns:
            logger.error("Required columns not found for movie counting")
            return 0
        
        filtered = df[
            (df['Gross_Numeric'] >= threshold) & 
            (df['Year'] < before_year)
        ]
        
        count = len(filtered)
        logger.info(f"Found {count} movies over ${threshold}B before {before_year}")
        return count
    
    def find_earliest_over_threshold(self, df: pd.DataFrame, threshold: float) -> str:
        """Find earliest film over gross threshold"""
        
        if 'Gross_Numeric' not in df.columns or 'Year' not in df.columns:
            logger.error("Required columns not found for earliest film search")
            return "Unknown"
        
        filtered = df[df['Gross_Numeric'] >= threshold]
        
        if filtered.empty:
            return "None found"
        
        earliest = filtered.loc[filtered['Year'].idxmin()]
        title = earliest.get('Title', 'Unknown')
        
        logger.info(f"Earliest film over ${threshold}B: {title}")
        return title
    
    def calculate_correlation(self, df: pd.DataFrame, col1: str, col2: str) -> float:
        """Calculate correlation between two columns"""
        
        if col1 not in df.columns or col2 not in df.columns:
            logger.error(f"Columns {col1} or {col2} not found")
            return 0.0
        
        # Remove NaN values
        clean_data = df[[col1, col2]].dropna()
        
        if len(clean_data) < 2:
            logger.error("Insufficient data for correlation calculation")
            return 0.0
        
        correlation = clean_data[col1].corr(clean_data[col2])
        
        logger.info(f"Correlation between {col1} and {col2}: {correlation}")
        return correlation
    
    def linear_regression(self, x: pd.Series, y: pd.Series) -> tuple:
        """Perform linear regression and return slope, intercept"""
        
        clean_data = pd.DataFrame({'x': x, 'y': y}).dropna()
        
        if len(clean_data) < 2:
            return 0.0, 0.0
        
        slope, intercept, r_value, p_value, std_err = stats.linregress(
            clean_data['x'], clean_data['y']
        )
        
        return slope, intercept