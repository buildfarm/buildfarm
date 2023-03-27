$(document).ready(function() {

  $.ajaxSetup({ cache: false });

  // Display widget status in new window in the status dashboard
  $('#widgetModal').on('show.bs.modal', function (event) {
    var button = $(event.relatedTarget)
    var widgetSrc = button.data('whatever')
    var modal = $(this)
    modal.find('.modal-img').attr('src', widgetSrc)
  })

  // Handle actionable buttons
  $("a.actionable-btn").click(function(e){
    e.preventDefault();
    $.ajax({
      type: "POST",
      beforeSend: function(request) {
        request.setRequestHeader("SecurityKey", securityKey);
      },
      url: $(this).attr("href"),
      success: function() {
        $('.toastSuccess').toast('show');
      },
      error: function (request, status, error) {
        $("div.ajax-error").html(request.responseText);
        $("#actionFailure").modal('show');
      }
    });
    return false;
  });


  // Handle actionable buttons with prompt
  $("a.actionable-btn-prompt").click(function(e){
    e.preventDefault();
    if (confirm("Are you sure?")) {
      $.ajax({
        type: "POST",
        beforeSend: function(request) {
          request.setRequestHeader("SecurityKey", securityKey);
        },
        url: $(this).attr("href"),
        success: function() {
          $('.toastSuccess').toast('show');
        },
        error: function (request, status, error) {
          $("div.ajax-error").html(request.responseText);
          $("#actionFailure").modal('show');
        }
      });
    }
    return false;
  });

  // Handle actionable buttons with a field value
  $("a.actionable-btn-with-value").click(function(e){
    e.preventDefault();
    $.ajax({
      type: "POST",
      beforeSend: function(request) {
        request.setRequestHeader("SecurityKey", securityKey);
      },
      url: $(this).attr("href") + $(this).closest(".actionable-container").find(".actionable-value").val(),
      success: function() {
        $('.toastSuccess').toast('show');
      },
      error: function (request, status, error) {
        $("div.ajax-error").html(request.responseText);
        $("#actionFailure").modal('show');
      }
    });
    return false;
  });

  // Handle actionable buttons with a field value as data
  $("a.actionable-btn-with-value-as-data").click(function(e){
    e.preventDefault();
    $.ajax({
      type: "POST",
      beforeSend: function(request) {
        request.setRequestHeader("SecurityKey", securityKey);
      },
      url: $(this).attr("href"),
      data: {payload : $(this).closest(".actionable-container").find(".actionable-value").val()},
      success: function() {
        $('.toastSuccess').toast('show');
      },
      error: function (request, status, error) {
        $("div.ajax-error").html(request.responseText);
        $("#actionFailure").modal('show');
      }
    });
    return false;
  });

});
