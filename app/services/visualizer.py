import matplotlib
matplotlib.use('Agg')  # Use non-interactive backend
import matplotlib.pyplot as plt
import seaborn as sns
import pandas as pd
import numpy as np
import base64
import io
from typing import Optional
import logging

logger = logging.getLogger(__name__)

class DataVisualizer:
    
    def __init__(self):
        # Set style
        plt.style.use('default')
        sns.set_palette("husl")
    
    async def create_scatterplot_with_regression(
        self, 
        df: pd.DataFrame, 
        x_col: str, 
        y_col: str, 
        regression_color: str = 'red',
        regression_style: str = 'dotted',
        max_size_kb: int = 100
    ) -> str:
        """Create scatterplot with regression line and return as base64 data URI"""
        
        # Clean data
        plot_data = df[[x_col, y_col]].dropna()
        
        if len(plot_data) < 2:
            logger.error("Insufficient data for plotting")
            return "data:image/png;base64,iVBORw0KGgoAAAANSUhEUgAAAAEAAAABCAYAAAAfFcSJAAAADUlEQVR42mNkYPhfDwAChwGA60e6kgAAAABJRU5ErkJggg=="
        
        # Create figure
        fig, ax = plt.subplots(figsize=(10, 6), dpi=100)
        
        # Create scatterplot
        ax.scatter(plot_data[x_col], plot_data[y_col], alpha=0.7, s=50)
        
        # Add regression line
        z = np.polyfit(plot_data[x_col], plot_data[y_col], 1)
        p = np.poly1d(z)
        x_line = np.linspace(plot_data[x_col].min(), plot_data[x_col].max(), 100)
        y_line = p(x_line)
        
        linestyle = '--' if regression_style == 'dotted' else '-'
        ax.plot(x_line, y_line, color=regression_color, linestyle=linestyle, linewidth=2)
        
        # Labels and title
        ax.set_xlabel(x_col)
        ax.set_ylabel(y_col)
        ax.set_title(f'{x_col} vs {y_col} with Regression Line')
        ax.grid(True, alpha=0.3)
        
        # Convert to base64
        buffer = io.BytesIO()
        
        # Try different quality settings to stay under size limit
        for quality in [95, 85, 75, 65, 55]:
            buffer.seek(0)
            buffer.truncate()
            
            plt.savefig(buffer, format='png', bbox_inches='tight', 
                       dpi=100, optimize=True)
            
            buffer.seek(0)
            img_data = buffer.read()
            
            # Check size
            if len(img_data) <= max_size_kb * 1024:
                break
        
        plt.close(fig)
        
        # Encode to base64
        img_base64 = base64.b64encode(img_data).decode('utf-8')
        data_uri = f"data:image/png;base64,{img_base64}"
        
        logger.info(f"Created plot with size: {len(img_data)} bytes")
        return data_uri