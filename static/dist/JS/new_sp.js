//准备视图对象
window.viewObj = {
    tbData: [{'name': '子品类', 'type': 1, 'rank': 1, 'state': 1}, {'name': '是否无效链接', 'type': 1, 'rank': 3, 'state': 1}, {'name': '品牌(不出数)', 'type': 1, 'rank': 0, 'state': 0}, {'name': '是否套包', 'type': 1, 'rank': 2, 'state': 0}, {'name': '是否人工答题', 'type': 1, 'rank': 4, 'state': 0}, {'name': '套包宝贝', 'type': 1, 'rank': 5, 'state': 0}, {'name': '店铺分类', 'type': 1, 'rank': 6, 'state': 0}, {'name': '疑似新品', 'type': 1, 'rank': 7, 'state': 0}, {'name': '包装规格', 'type': 1, 'rank': 8, 'state': 0}, {'name': '单包抽数1', 'type': 1, 'rank': 9, 'state': 0}, {'name': '单包抽数2', 'type': 1, 'rank': 10, 'state': 0}, {'name': '单包抽数3', 'type': 1, 'rank': 11, 'state': 0}, {'name': '单包抽数4', 'type': 1, 'rank': 12, 'state': 0}, {'name': '规格1', 'type': 1, 'rank': 13, 'state': 0}, {'name': '规格2', 'type': 1, 'rank': 14, 'state': 0}, {'name': '规格3', 'type': 1, 'rank': 15, 'state': 0}, {'name': '规格4', 'type': 1, 'rank': 16, 'state': 0}],
    typeData: [
        {id: 1, name: '普通属性'},
        {id: 2, name: '度量属性(聚合值)'},
    ],
    renderSelectOptions: function(data, settings){
        settings =  settings || {};
        var valueField = settings.valueField || 'value',
            textField = settings.textField || 'text',
            selectedValue = settings.selectedValue || "";
        var html = [];
        for(var i=0, item; i < data.length; i++){
            item = data[i];
            html.push('<option value="');
            html.push(item[valueField]);
            html.push('"');
            if(selectedValue && item[valueField] == selectedValue ){
                html.push(' selected="selected"');
            }
            html.push('>');
            html.push(item[textField]);
            html.push('</option>');
        }
        return html.join('');
    }
};