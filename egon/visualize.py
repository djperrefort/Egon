"""Launches a Dash app for visualizing the status of a pipeline"""

from typing import List

import dash
import dash_cytoscape as cyto
import dash_html_components as html

from egon.pipeline import Pipeline

DEFAULT_LAYOUT = 'breadthfirst'


class APP:
    """Application for visualizing an analysis pipeline"""

    def __init__(self, pipeline: Pipeline) -> None:
        self._pipeline = pipeline
        pipeline.validate()

    def _get_pipeline_elements(self) -> List[dict]:
        """Return the pipeline elements to monitor"""

        raise NotImplementedError

    def _build_overview_section(
            self,
            width: str = '1000px',
            height: str = '1000px',
            layout: str = DEFAULT_LAYOUT
    ) -> html.Div:
        """Build the `Pipeline Overview` section of the dashboard"""

        # Useful docs: https://dash.plotly.com/cytoscape
        cytoscape = cyto.Cytoscape(
            id='overview-component',
            elements=self._get_pipeline_elements(),
            layout={'name': layout},
            style={'width': width, 'height': height}
        )

        return html.Div([
            html.H1('Pipeline Overview', id='overview-header'),
            cytoscape
        ], id='overview-section')

    def _build_detailed_node_section(self) -> html.Div:
        return html.Div([], id='node-section')

    def _build_system_monitor_section(self) -> html.Div:
        return html.Div([], id='system-monitor-section')

    def run(
            self,
            host: str = '127.0.0.1',
            port: int = 8050,
            proxy: str = None
    ) -> None:
        """Launch the pipeline inspector

        Args:
            host: Host IP used to serve the application
            port: Port used to serve the application
            proxy: Optional url for serving the application externally
        """

        app = dash.Dash(__name__)
        app.layout = html.Div([
            self._build_overview_section(),
            self._build_detailed_node_section(),
            self._build_system_monitor_section()
        ])

        app.run_server(host, port, proxy, use_reloader=False)
