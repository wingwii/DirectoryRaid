
function BuildDirNode(name, path) {
    var html = "";
    html += "<div class=\"DirNode\" >";
    html += "<div class=\"DirName\" onclick=\"ExpandDir(this, \'";
    html += path;
    html += "\')\" >";
    html += name;
    html += "</div><div>";
    html += "<div class=\"DirNodeMargin\" >&nbsp;</div>";
    html += "<div class=\"DirNodeCnt\" ></div>";
    html += "</div></div>";
    return html;
}

function BuildFileNode(name, path) {
    var html = "";
    html += "<div class=\"FileName\" onclick=\"OpenDbFile(\'";
    html += path;
    html += "\')\" >";
    html += name;
    html += "</div>";
    return html;
}

function ExpandDirCompleted(node, data) {
    node.FileTreeNodeData = data;

    var divCnt = node.parentNode.querySelectorAll("div")[3];
    var html = "";
    if (data != null) {
        var fileList = data.subdirs;
        var n = fileList.length;
        for (var i = 0; i < n; ++i) {
            var fi = fileList[i];
            html += BuildDirNode(fi[0], fi[1]);
        }

        fileList = data.files;
        n = fileList.length;
        for (var i = 0; i < n; ++i) {
            var fi = fileList[i];
            html += BuildFileNode(fi[0], fi[1]);
        }

    }
    divCnt.innerHTML = html;
}

function ExpandDir(node, path) {
    if (!node.hasOwnProperty('FileTreeNodeData')) {
        node.FileTreeNodeData = null;
    }

    if (node.FileTreeNodeData != null) {
        ExpandDirCompleted(node, null);
        return;
    }

    jQuery.get({
        url: "api/LocalFileBrowser/" + path,
        dataType: "json",
        success: function (data) { ExpandDirCompleted(node, data); }
    });
}

function OpenDbFile(path) {
    var url = 'Explorer?path=' + path;
    window.open(url, '_blank').focus();
}
