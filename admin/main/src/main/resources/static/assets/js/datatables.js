// Call the dataTables jQuery plugin
$(document).ready(function() {
  $('#dataTable').dataTable( {
    "pageLength": 100,
    "order": []
  });
  $('#dataTableBasic').dataTable( {
    "paging": false,
    "searching": false,
    "order": []
  });
  $('#dataTableServers').dataTable( {
    "pageLength": 100,
    "order": []
  });
  $('#dataTableWorkers').dataTable( {
    "pageLength": 100,
    "order": []
  });
  $('#dataTableGpuWorkers').dataTable( {
    "pageLength": 100,
    "order": []
  });
});