import time
import logging
import sys

ptycho_flow_definition = {
  #"definition":{
      "Comment": "Ptychographic reconstruction workflow",
      "StartAt": "Transfer",
      "States": {
        "Transfer": {
          "Comment": "Initial transfer",
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
          "ResultPath": "$.Transfer1Result",
          "WaitTime": 14400,
          "Next": "PosTransfer"
        },
        "PosTransfer": {
          "Comment": "Transfer position file",
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
          "ResultPath": "$.Transfer1Result",
          "WaitTime": 14400,
          "Next": "Analyze"
        },
        "Analyze": {
          "Comment": "Run a funcX function",
          "Type": "Action",
          #"ActionUrl": "https://api.funcx.org/automate",
            "ActionUrl": "https://automate.funcx.org",
          #"ActionScope": "https://auth.globus.org/scopes/facd7ccc-c5f4-42aa-916b-a0e270e2c2a9/automate2",
            "ActionScope": "https://auth.globus.org/scopes/b3db7e59-a6f1-4947-95c2-59d6b7a70f8c/action_all",
          "Parameters": {
              "tasks": [{
                "endpoint.$": "$.input.fx_ep",
                "function.$": "$.input.fx_id",
                "payload.$": "$.input.params"
            }]
          },
          "ResultPath": "$.AnalyzeResult",
          "WaitTime": 14400,
          "Next": "Transfer2"
        },
        "Transfer2": {
          "Comment": "Return transfer",
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
                "recursive": True #False
              }
            ]
          },
          "ResultPath": "$.Transfer2Result",
          "WaitTime": 14400,
          "End": True
        },
      },
  #},
  #"input_schema": {}
}

def fx_get_result(wfunc_id, fxc, times=None, delay=2):
    fx_res = None
    while True:
      try:
          time.sleep(delay)
          fx_res = fxc.get_result(wfunc_id)
          if isinstance(times, int): 
            if times>0 : times-=1
            else: return fx_res 
      except KeyboardInterrupt:
          logging.debug(f"Keyboard interrupt is caught: {sys.exc_info()[0]}")
          break
      except:
          logging.debug(f"Pending results: {sys.exc_info()[0]}")
          continue
      return fx_res