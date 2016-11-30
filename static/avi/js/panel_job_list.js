// This must be included for javascript to safely post to Django
function csrfSafeMethod(method){
    return (/^(GET|HEAD|OPTIONS|TRACE)$/.test(method));
}
$.ajaxSetup({
    beforeSend: function(xhr, settings){
        var csrftoken = $.cookie('csrftoken');
        if (!csrfSafeMethod(settings.type) && !this.crossDomain) {
            xhr.setRequestHeader("X-CSRFToken", csrftoken);
        }
    }
});


$(document).ready(function() {
    setup_jobtable();
});

/*
https://datatables.net/examples/basic_init/hidden_columns.html
*/
var jobTable;

function setup_jobtable(){
    jobTable = $('#job_table').DataTable({
        "ajax": {
            "url": "/avi/job_list/.json", 
            "dataSrc": ""
        },
        "columns": [
            { "data": "job_id" },
            { "data": "task_name" },
            { "data": "created" },
            { "data": "pipeline_state.last_activity_time" },
            { "data": "pipeline_state.progress" },
            { "data": "pipeline_state.state" },
            { "data": "result_path" },
            { "data": "pipeline_state.exception" }
        ],
        "columnDefs": [{
                "targets": [ 6 ],
                "visible": false
        }],
        "order": [[ 0, "desc" ]],
        "drawCallback": function( settings ) {
            restyle_table(jobTable);
        }
    });

    setInterval( function () {
        if ($('#job_table').is(':visible')) {
            jobTable.ajax.reload(null, false);
        }
    }, 3000 );
}

function restyle_table(jobTable){
    if (typeof(jobTable) != 'undefined'){
        jobTable.rows().every(function(){
            var row_data = this.data();
            var row_node = this.node();
            style_row(row_data, row_node);
        });
    }
    bind_result_buttons();
}

function style_row(row_data, row_node){
    var job_state = row_data.pipeline_state.state;
    var row = $(row_node); // make it a jquery object for modifying
    if (job_state == 'SUCCESS'){
        row.addClass("success");
        row.find('td:eq(6)').html('<button class="btn btn-success btn-block btn-xs" name="result_view_btn">Results</button>');
    } 
    else if (job_state == 'FAILURE'){
        row.addClass("danger");
        row.find('td:eq(6)').html('<button class="btn btn-danger btn-block btn-xs" name="exception_view_button">View Exception</button>');
    } 
    else {
        row.addClass("info");
        row.find('td:eq(6)').html('<button class="btn btn-info btn-block btn-xs" disabled>Pending</button>');
    }

    var job_percentage = parseFloat(row_data.pipeline_state.progress);
    row.find('td:eq(4)').html(generate_progress_bar(job_percentage));
}

function generate_progress_bar(job_percentage){
    return '<div class="progress" style="margin-bottom:0px;"><div class="progress-bar" role="progressbar"' +
        'aria-valuenow="'+job_percentage+'" aria-valuemin="0" aria-valuemax="100" ' +
        'style="width: '+job_percentage+'%;">'+job_percentage+'% </div></div>';
}

function bind_result_buttons(){
    $('button[name="result_view_btn"]').parent().click(function(){
        var row = get_row(this);
        var data_url = "/avi/result/" + row.data().job_id;
        window.location = data_url;
        // $('#result-tab').tab('show');
        // view_result(data_url);
    })
    $('button[name="exception_view_button"]').parent().click(function(){
        var row = get_row(this);
        var exception = row.data().pipeline_state.exception;
        view_exception(exception);
    })
}

function get_row(cell){
    // This returns the actual row that Datatables is aware of (both the original data element, and the HTML node)
    var rowIndex = jobTable.cell(cell).index().row;
    var row = jobTable.row(rowIndex);
    return row;
}

function view_exception(exceptionstr){
    $('#modal .modal-title').html("Exception");
    $('#modal .modal-body').html("<pre>"+exceptionstr+"</pre>");
    $('#modal').modal('show');
}