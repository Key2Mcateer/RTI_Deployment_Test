from datetime import datetime
import fabric.functions as fn
import logging

udf = fn.UserDataFunctions()

def pipeline_execution_link(WorkspaceID: str, PipelineID: str, PipelineRunID: str) -> str:
  monitoring_url = f"https://app.fabric.microsoft.com/workloads/data-pipeline/monitoring/workspaces/{WorkspaceID}/pipelines/{PipelineID}/{PipelineRunID}"
  return monitoring_url

@udf.function()
def generate_teams_message(Status: str, PipelineID: str, PipelineRunID: str, PipelineName: str, WorkspaceID: str, WorkspaceName: str, 
                           StartTime: str, EndTime: str, FailingObject: str, ErrorMessage: str) -> str:
  #Calculate duration
  s_time = datetime.strptime(StartTime, "%Y-%m-%d %H:%M:%S")
  e_time = datetime.strptime(EndTime, "%Y-%m-%d %H:%M:%S")
  time_difference = e_time - s_time
  duration_seconds = time_difference.seconds
  
  # Determine teams status variables
  error_info = ""
  if Status.lower() in ["success", "succeeded"]:
      status_message = "Success"
      status_color = "#229954"
  else:
      status_message = "Failure"
      status_color = "#A93226"
      error_info  = f"""<b>Failing Object:</b> {FailingObject}<br>"""
      error_info += f"""<b>Error Message:</b> {ErrorMessage}<br>"""
        
  # Convert duration to hours, minutes, and seconds
  if duration_seconds >= 3600:
      hours = duration_seconds // 3600
      minutes = (duration_seconds % 3600) // 60
      duration_value = f"{hours} hours {minutes} minutes"
  elif duration_seconds >= 60:
      minutes = duration_seconds // 60
      seconds = duration_seconds % 60
      duration_value = f"{minutes} minutes {seconds} seconds"
  else:
      seconds = duration_seconds
      duration_value = f"{seconds} seconds"

  #Get Pipeline Execution Link
  monitoring_url = pipeline_execution_link(WorkspaceID, PipelineID, PipelineRunID)

  #Create HTML
  html_message  = f"""<b style="color:{status_color};"> Pipeline {status_message} Alert!</b><br><br>"""
  html_message += f"""<b>Pipeline Name:</b> {PipelineName}<br>"""
  html_message += f"""<b>Workspace:</b> {WorkspaceName}<br>"""
  html_message += f"""<b>Start Time:</b> {StartTime}<br>"""
  html_message += f"""<b>End Time:</b> {EndTime}<br>"""
  html_message += f"""<b>Duration:</b> {duration_value}<br>"""
  html_message += f"""{error_info}<br>"""
  html_message += f"""<a href="{monitoring_url}">🔗 View Details</a><br><br>"""
  html_message += f"""<sub>sent from the auditing system</sub>"""
  
  return html_message

@udf.function()
def generate_email_message(Status: str, PipelineID: str, PipelineRunID: str, PipelineName: str, WorkspaceID: str, WorkspaceName: str, 
                           StartTime: str, EndTime: str, FailingObject: str, ErrorMessage: str) -> str:
  #Calculate duration
  s_time = datetime.strptime(StartTime, "%Y-%m-%d %H:%M:%S")
  e_time = datetime.strptime(EndTime, "%Y-%m-%d %H:%M:%S")
  time_difference = e_time - s_time
  duration_seconds = time_difference.seconds

  # Determine email status variables
      # Determine email subject and status message
  if Status.lower() in ["success", "succeeded"]:
      subject = f"Pipeline Success - {PipelineName}"
      status_message = "Succeeded"
      status_color = "#229954"
  else:
      subject = f"Pipeline Failure - {PipelineName}"
      status_message = "Failed"
      status_color = "#A93226"
  
  # Convert duration to hours, minutes, and seconds
  if duration_seconds >= 3600:
      hours = duration_seconds // 3600
      minutes = (duration_seconds % 3600) // 60
      duration_value = f"{hours} hours {minutes} minutes"
  elif duration_seconds >= 60:
      minutes = duration_seconds // 60
      seconds = duration_seconds % 60
      duration_value = f"{minutes} minutes {seconds} seconds"
  else:
      seconds = duration_seconds
      duration_value = f"{seconds} seconds"

  #Get Pipeline Execution Link
  monitoring_url = pipeline_execution_link(WorkspaceID, PipelineID, PipelineRunID)
  
  # Build the full HTML message
  #Opening tags
  html_message  = "<html>"
  html_message += "<head>"
  html_message += "<style>"

  #CSS Styles
  html_message += ".body {margin: 0;padding: 0;font-family: Poppins, Arial, sans-serif;}"
  html_message += ".header {background-color: #345C72;padding: 40px;text-align: center;color: white;font-size: 24px;font-weight: bold;}"
  html_message += ".footer {background-color: #333333;padding: 40px;text-align: center;color: white;font-size: 14px;}"
  html_message += ".main {width: 700px;border-collapse: collapse;}"
  html_message += ".field_name {background-color: #345C72;color: white;padding: 7px;border: solid 1px #345C72;width: 150px;vertical-align: top;}"
  html_message += ".field_value {padding: 7px;border: solid 1px #345C72;vertical-align: top;}"
  html_message += ".status_message {width: 200px; color: white; font-weight: bold; padding: 10px; text-align: center; background-color: red;}"
  html_message += "</style>"
  html_message += "</head>"
  html_message += f"<body class='body'>"
  html_message += f"<table width='100%' cellpadding='0' cellspacing='0' border='0'>"
  html_message += "<tr>"
  html_message += "<td align='center'>"
  html_message += "<table width='700' cellpadding='0' cellspacing='0' border='0' style='border:1px solid #ddd;'>"
  html_message += "<tr>"

  #Email Header
  html_message += "<td class='header'>"
  html_message += f"{subject}"
  html_message += "</td>"
  html_message += "</tr>"
  html_message += "<tr>"
  html_message += "<td style='padding: 20px; font-size: 16px;'>"

  #Failure or Success Message
  html_message += "<table>"
  html_message += "<tr>"
  html_message += f"<td style = 'width:200px;color:white;font-weight:bold;padding: 10px;text-align:center;background-color:{status_color};'>"
  html_message += f"Pipeline {status_message}"
  html_message += "</td>"
  html_message += "</tr>"
  html_message += "</table>"
  html_message += "</br>"

  #Job Detail Fields
  html_message += "<table style = 'border-collapse:collapse'>"
  html_message += "<tr>"
  html_message += "<td class='field_name'>Pipeline Name</td>"
  html_message += f"<td class='field_value'>{PipelineName}</td>"
  html_message += "</tr>"
  html_message += "<tr>"
  html_message += "<td class='field_name'>Workspace</td>"
  html_message += f"<td class='field_value'>{WorkspaceName}</td>"
  html_message += "</tr>"
  html_message += "<tr>"
  html_message += "<td class='field_name'>Start Time</td>"
  html_message += f"<td class='field_value'>{StartTime}</td>"
  html_message += "</tr>"
  html_message += "<tr>"
  html_message += "<td class='field_name'>End Time</td>"
  html_message += f"<td class='field_value'>{EndTime}</td>"
  html_message += "</tr>"
  html_message += "<tr>"
  html_message += "<td class='field_name'>Duration</td>"
  html_message += f"<td class='field_value'>{duration_value}</td>"
  html_message += "</tr>"

  #Add fields for failures
  if status_message == "Failed":
    html_message += "<tr>"
    html_message += "<td class='field_name'>Failing Object</td>"
    html_message += f"<td class='field_value'>{FailingObject}</td>"
    html_message += "</tr>"
    html_message += "<tr>"
    html_message += "<td class='field_name'>Error Message</td>"
    html_message += f"<td class='field_value'>{ErrorMessage}</td>"
    html_message += "</tr>"

  html_message += "</table>"
  html_message += "</br>"

  #View Job Execution Button
  html_message += "<table align='center'>"
  html_message += "<tr>"
  html_message += "<td align='center' style='background-color: #345C72; padding: 10px 20px; border-radius: 5px;'>"
  html_message += f"<a href='{monitoring_url}' target='_blank' style='color: #ffffff; text-decoration: none; font-weight: bold;'>View Job Execution</a>"
  html_message += "</td>"
  html_message += "</tr>"
  html_message += "</table>"
  html_message += "</td>"
  html_message += "</tr>"
  
  #Footer
  html_message += "<tr>"
  html_message += "<td class='footer'>"
  html_message += "Sent from the auditing system"
  html_message += "</td>"
  html_message += "</tr>"
  html_message += "</table>"
  html_message += "</td>"
  html_message += "</tr>"
  html_message += "</table>"
  html_message += "</body>"
  html_message += "</html>"
  
  return html_message
