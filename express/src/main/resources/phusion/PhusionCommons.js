
var TimeUtil = {
    _f: function(n) { return n>9 ? ""+n : "0"+n },

    timestampToStr19: function(timestamp, shift) {
        shift = shift || 0;
        var date;

        if (timestamp) date = new Date(timestamp+shift);
        else date = new Date((new Date()).getTime()+shift);

        var f = TimeUtil._f;

        var parts = [
            date.getFullYear(),
            "-",
            f(date.getMonth()+1),
            "-",
            f(date.getDate()),
            " ",
            f(date.getHours()),
            ":",
            f(date.getMinutes()),
            ":",
            f(date.getSeconds())
        ];
        return parts.join('');
    },

    timestampToStr10: function(timestamp, shift) {
        shift = shift || 0;
        var date;

        if (timestamp) date = new Date(timestamp+shift);
        else date = new Date((new Date()).getTime()+shift);

        var f = TimeUtil._f;

        var parts = [
            date.getFullYear(),
            "-",
            f(date.getMonth()+1),
            "-",
            f(date.getDate())
        ];
        return parts.join('');
    }

};

exports.TimeUtil = TimeUtil;
