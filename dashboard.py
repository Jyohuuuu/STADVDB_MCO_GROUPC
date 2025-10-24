import dash
from dash import dcc, html, Input, Output, callback
import plotly.express as px
import plotly.graph_objects as go
from reports import gdp_population_correlation_report, cost_of_living_vs_purchasing_power_report, climate_quality_vs_economic_development_report, traffic_commute_category_report
import pandas as pd
import random


# Get Report Data
gdp_pop_df = gdp_population_correlation_report()
cost_living_df = cost_of_living_vs_purchasing_power_report()
climate_gdp_df = climate_quality_vs_economic_development_report()
traffic_commute_df = traffic_commute_category_report()

app = dash.Dash(__name__)

# GDP vs Population Scatter Plot
gdp_pop_fig = px.scatter(
    gdp_pop_df, 
    x="population", 
    y="gdp_usd", 
    color="country_name", 
    hover_name="country_name",
    size="population", 
    size_max=60, 
    log_x=True, 
    log_y=True,
    title="GDP vs. Population by Country",
    labels={
        "population": "Population (log scale)",
        "gdp_usd": "GDP in USD (log scale)",
        "country_name": "Country"
    }
)

cost_living_clean = cost_living_df.dropna(subset=['country_name', 'avg_cost_of_living', 'avg_purchasing_power'])
available_countries = sorted(cost_living_clean['country_name'].unique())

random.seed(42)
default_countries = random.sample(available_countries, min(10, len(available_countries)))

# Prepare climate-GDP data 
climate_gdp_clean = climate_gdp_df.dropna(subset=['country_name', 'total_gdp_usd', 'climate_quality_2025'])
climate_gdp_clean = climate_gdp_clean[climate_gdp_clean['country_name'].notna()]

# Prepare heatmap countries list
available_heatmap_countries = sorted(climate_gdp_clean['country_name'].unique())

# Traffic Commute Category Treemap
traffic_commute_treemap_fig = px.treemap(
    traffic_commute_df.sort_values('sort_order'),
    path=[px.Constant("Traffic Commute Categories"), 'traffic_commute_category'],
    values='total_population',
    color='avg_gdp_per_capita',
    color_continuous_scale='Viridis',
    title="Traffic Commute Categories: Population and Average GDP Per Capita",
    hover_data={'total_population': ':,.0f', 'avg_gdp_per_capita': ':,.2f'}
)

app.layout = html.Div([
    html.H1("Country Metrics Dashboard", style={'textAlign': 'center', 'marginBottom': 30}),
    
    html.Div([
        html.P("Note: Some countries may have incomplete data and will not appear in all visualizations.", 
               style={'textAlign': 'center', 'color': '#666', 'fontStyle': 'italic', 'marginBottom': 30})
    ]),
    
    # GDP vs Population Section
    html.Div([
        html.H2("Economic Analysis"),
        dcc.Graph(
            id='gdp-vs-population-scatter',
            figure=gdp_pop_fig
        )
    ], style={'marginBottom': 40}),
    
    # Cost of Living Section
    html.Div([
        html.H2("Quality of Life Analysis"),
        html.Div([
            html.Label("Select Countries:", style={'fontWeight': 'bold', 'marginBottom': 10}),
            dcc.Dropdown(
                id='country-dropdown',
                options=[{'label': country, 'value': country} for country in available_countries],
                value=default_countries,
                multi=True,
                placeholder="Select countries to display...",
                style={'marginBottom': 20}
            )
        ]),
        dcc.Graph(
            id='cost-living-chart'
        )
    ], style={'marginBottom': 40}),
    
    # Climate Quality Heatmap Section
    html.Div([
        html.H2("Climate Quality vs Economic Development Heatmap"),
        html.Div([
            html.Label("Select Countries for Heatmap:", style={'fontWeight': 'bold', 'marginBottom': 10}),
            dcc.Dropdown(
                id='heatmap-country-dropdown',
                options=[{'label': country, 'value': country} for country in available_heatmap_countries],
                value=available_heatmap_countries[:15],
                multi=True,
                placeholder="Select countries to display in heatmap...",
                style={'marginBottom': 20}
            )
        ]),
        dcc.Graph(
            id='climate-heatmap'
        )
    ], style={'marginBottom': 40}),

    # Traffic Commute Category Treemap Section
    html.Div([
        html.H2("Traffic Commute Category Treemap"),
        dcc.Graph(
            id='traffic-commute-treemap',
            figure=traffic_commute_treemap_fig
        )
    ], style={'marginBottom': 40})
], style={'padding': 20})

#for updating the cost of living chart based on selected countries
@callback(
    Output('cost-living-chart', 'figure'),
    Input('country-dropdown', 'value')
)
def update_cost_living_chart(selected_countries):
    if not selected_countries:
        fig = px.bar(title="Please select countries to display")
        return fig
    
    filtered_df = cost_living_clean[cost_living_clean['country_name'].isin(selected_countries)]
    
    total_countries_with_data = len(available_countries)
    chart_title = f"Average Cost of Living vs Purchasing Power by Country<br><sub>Data available for {total_countries_with_data} countries</sub>"
    
    fig = px.bar(
        filtered_df,
        x="country_name",
        y=["avg_cost_of_living", "avg_purchasing_power"],
        title=chart_title,
        labels={
            "value": "Index Value",
            "country_name": "Country",
            "variable": "Metric"
        },
        barmode="group",
        color_discrete_map={
            "avg_cost_of_living": "#FF6B6B",
            "avg_purchasing_power": "#4ECDC4"
        }
    )
    fig.update_xaxes(tickangle=45)
    fig.update_layout(
        xaxis_title="Country",
        yaxis_title="Index Value",
        legend_title="Metric"
    )
    
    return fig

@callback(
    Output('climate-heatmap', 'figure'),
    Input('heatmap-country-dropdown', 'value')
)
def update_climate_heatmap(selected_countries):
    if not selected_countries:
        fig = px.imshow([[0]], title="Please select countries to display")
        return fig
    
    filtered_df = climate_gdp_clean[climate_gdp_clean['country_name'].isin(selected_countries)]
    
    if filtered_df.empty:
        fig = px.imshow([[0]], title="No data available for selected countries")
        return fig
    
    heatmap_data = filtered_df.pivot_table(
        values='development_efficiency_ratio', 
        index='country_name', 
        columns='year_value', 
        aggfunc='mean'
    ).fillna(0)
    
    if heatmap_data.empty:
        fig = px.imshow([[0]], title="No data available for selected countries")
        return fig
    
    fig = px.imshow(
        heatmap_data.values,
        labels=dict(x="Year", y="Country", color="Development Efficiency Ratio"),
        x=heatmap_data.columns,
        y=heatmap_data.index,
        title="Climate Quality vs Economic Development Efficiency (2020-2025)<br><sub>Higher values indicate better economic efficiency relative to climate quality</sub>",
        color_continuous_scale="RdYlGn",
        aspect="auto"
    )
    
    fig.update_layout(
        xaxis_title="Year",
        yaxis_title="Country",
        height=max(400, len(heatmap_data.index) * 35),
        coloraxis_colorbar=dict(
            title="Development Efficiency Ratio"
        )
    )
    
    fig.update_traces(
        text=heatmap_data.round(2).values,
        texttemplate="%{text}",
        textfont={"size": 10},
        hovertemplate="<b>%{y}</b><br>Year: %{x}<br>Efficiency Ratio: %{z:.2f}<extra></extra>"
    )
    
    return fig


if __name__ == '__main__':
    app.run(debug=True)
