from gladier import GladierBaseClient, GladierBaseTool, generate_flow_definition
import os
import yaml

from apwf.tools.fx_comp_helper import ptycho_func

class GladierTransferData(GladierBaseTool):
    flow_definition = {
        "Comment": "Transfer a file or folder using Globus Transfer",
        "StartAt": "TransferData",
        "States": {
            "TransferData": {
                "Comment": "Transfer diffraction pattern data",
                "Type": "Action",
                "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
                "ActionScope": "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer",
                "Parameters": {
                    "source_endpoint_id.$": "$.input.source_endpoint",
                    "destination_endpoint_id.$": "$.input.dest_endpoint",
                    "transfer_items": [
                    {
                        "source_path.$": "$.input.source_path",
                        "destination_path.$": "$.input.dest_path",
                        "recursive": True
                    }
                    ]
                },
                "ResultPath": "$.TransferData",
                "WaitTime": 14400,
                "End": True
            }
        }
    }

    flow_input = {}
    required_input = [
        'source_endpoint',
        'dest_endpoint',
        'source_path',
        'dest_path',
    ]


class GladierTransferPosition(GladierBaseTool):
    flow_definition = {
        "Comment": "Transfer a file or folder using Globus Transfer",
        "StartAt": "TransferPos",
        "States": {
            "TransferPos": {
                "Comment": "Transfer position data",
                "Type": "Action",
                "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
                "ActionScope": "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer",
                "Parameters": {
                    "source_endpoint_id.$": "$.input.source_endpoint",
                    "destination_endpoint_id.$": "$.input.dest_endpoint",
                    "transfer_items": [
                    {
                        "source_path.$": "$.input.source_pos_path",
                        "destination_path.$": "$.input.dest_pos_path",
                        "recursive": False
                    }
                    ]
                },
                "ResultPath": "$.TransferPos",
                "WaitTime": 14400,
                "End": True
            }
        }
    }

    flow_input = {}
    required_input = [
        'source_endpoint',
        'dest_endpoint',
        'source_pos_path',
        'dest_pos_path',
    ]

class GladierTransferResults(GladierBaseTool):
    flow_definition = {
        "Comment": "Transfer a file or folder using Globus Transfer",
        "StartAt": "TransferResults",
        "States": {
            "TransferResults": {
                "Comment": "Return results back",
                "Type": "Action",
                "ActionUrl": "https://actions.automate.globus.org/transfer/transfer",
                "ActionScope": "https://auth.globus.org/scopes/actions.globus.org/transfer/transfer",
                "Parameters": {
                    "source_endpoint_id.$": "$.input.dest_endpoint",
                    "destination_endpoint_id.$": "$.input.source_endpoint",
                    "transfer_items": [
                    {
                        "source_path.$": "$.input.result_path",
                        "destination_path.$": "$.input.source_result_path",
                        "recursive": True
                    }
                    ]
                },
                "ResultPath": "$.TransferResults",
                "WaitTime": 14400,
                "End": True
            }
        }
    }

    flow_input = {}
    required_input = [
        'source_endpoint',
        'dest_endpoint',
        'result_path',
        'source_result_path',
    ]

class GladierAnalyze(GladierBaseTool):
    funcx_functions = [ptycho_func]

    required_input = [ 
        'data',
        'fx_ep' # FuncX Endpoint ID
        ]


@generate_flow_definition
class GladierPtychoClient(GladierBaseClient):
    configfile = f"{os.getcwd()}/ptycho_wf_config.yml"
    wf_config= yaml.safe_load(open(configfile, 'r'))

    app_name=wf_config['flow']['sample_name']

    gladier_tools = [
        'GladierTransferData',      # Transfer diffraction data
        'GladierTransferPosition',  # Transfer position
        'GladierAnalyze',           # Run funcx function
        'GladierTransferResults',   # Transfer results back
    ]
