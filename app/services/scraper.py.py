import requests
from bs4 import BeautifulSoup
import pandas as pd
import re
import asyncio
from typing import Dict, List
import logging

logger = logging.getLogger(__name__)

class DataScraper:
    def __init__(self):
        self.session = requests.Session()
        self.session.headers.update({
            'User-Agent': 'Mozilla/5.0 (Windows NT 10.0; Win64; x64) AppleWebKit/537.36'
        })
    
    async def scrape_wikipedia_movies(self, url: str) -> pd.DataFrame:
        """Scrape highest-grossing films from Wikipedia"""
        
        response = self.session.get(url)
        response.raise_for_status()
        
        soup = BeautifulSoup(response.content, 'html.parser')
        
        # Find the main table
        table = soup.find('table', class_='wikitable')
        if not table:
            raise ValueError("Could not find the movies table on Wikipedia")
        
        # Parse table rows
        rows = []
        headers = []
        
        # Get headers
        header_row = table.find('tr')
        for th in header_row.find_all(['th', 'td']):
            headers.append(th.get_text(strip=True))
        
        # Get data rows
        for row in table.find_all('tr')[1:]:  # Skip header
            cells = row.find_all(['td', 'th'])
            if len(cells) >= len(headers):
                row_data = []
                for cell in cells[:len(headers)]:
                    text = cell.get_text(strip=True)
                    row_data.append(text)
                rows.append(row_data)
        
        # Create DataFrame
        df = pd.DataFrame(rows, columns=headers[:len(rows[0])] if rows else headers)
        
        # Clean and process the data
        df = self._clean_movie_data(df)
        
        logger.info(f"Scraped {len(df)} movies from Wikipedia")
        return df
    
    def _clean_movie_data(self, df: pd.DataFrame) -> pd.DataFrame:
        """Clean and process the movie data"""
        
        # Find relevant columns
        rank_col = None
        title_col = None
        year_col = None
        gross_col = None
        peak_col = None
        
        for col in df.columns:
            col_lower = col.lower()
            if 'rank' in col_lower:
                rank_col = col
            elif 'title' in col_lower or 'film' in col_lower:
                title_col = col
            elif 'year' in col_lower:
                year_col = col
            elif 'worldwide gross' in col_lower or 'total gross' in col_lower:
                gross_col = col
            elif 'peak' in col_lower:
                peak_col = col
        
        # Standardize column names
        column_mapping = {}
        if rank_col:
            column_mapping[rank_col] = 'Rank'
        if title_col:
            column_mapping[title_col] = 'Title'
        if year_col:
            column_mapping[year_col] = 'Year'
        if gross_col:
            column_mapping[gross_col] = 'Gross'
        if peak_col:
            column_mapping[peak_col] = 'Peak'
        
        df = df.rename(columns=column_mapping)
        
        # Clean numeric columns
        if 'Rank' in df.columns:
            df['Rank'] = pd.to_numeric(df['Rank'].str.extract('(\d+)')[0], errors='coerce')
        
        if 'Gross' in df.columns:
            # Extract numeric values from gross (remove $, commas, etc.)
            df['Gross_Numeric'] = df['Gross'].str.replace(r'[^\d.]', '', regex=True)
            df['Gross_Numeric'] = pd.to_numeric(df['Gross_Numeric'], errors='coerce') / 1_000_000_000  # Convert to billions
        
        if 'Peak' in df.columns:
            df['Peak'] = pd.to_numeric(df['Peak'].str.extract('(\d+)')[0], errors='coerce')
        
        if 'Year' in df.columns:
            df['Year'] = pd.to_numeric(df['Year'].str.extract('(\d{4})')[0], errors='coerce')
        
        return df.dropna()