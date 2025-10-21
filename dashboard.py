import dash
from dash import dcc
from dash import html
import plotly.express as px
from reports import gdp_population_correlation_report


df = gdp_population_correlation_report()


app = dash.Dash(__name__)


fig = px.scatter(df, x="population", y="gdp_usd", color="country_name", hover_name="country_name",
                 size="population", size_max=60, log_x=True, log_y=True,
                 title="GDP vs. Population")


app.layout = html.Div([
    html.H1("Country Metrics Dashboard"),
    dcc.Graph(
        id='gdp-vs-population-scatter',
        figure=fig
    )
])


if __name__ == '__main__':
    app.run(debug=True)
