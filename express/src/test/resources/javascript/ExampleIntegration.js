
if (transaction.step === "02") {
    var msg = transaction.msg;
    var discountHours = transaction.config.DiscountHours;

    var resultMsg = {};

    if (msg.mobile && msg.powerCharged>20) {
        resultMsg.plateNum = msg.vehicle;
        resultMsg.discountType = "duration";
        resultMsg.discountValues = discountHours;
        resultMsg.expireTime = dateToString(new Date((new Date()).valueOf()+24*60*60*1000));

        transaction.msg = resultMsg;
        transaction.moveToStep("03");
    }
    else {
        resultMsg.status = "FAILED";
        resultMsg.description = "Not match the condition";

        transaction.msg = resultMsg;
        transaction.moveToStep("04");
    }
}
else if (transaction.step === "04") {
    if (transaction.stepFrom === "03") {
        var msg = transaction.msg;
        var resultMsg = {};

        if (msg.status === "0") {
            resultMsg.status = "SUCCESS";
            resultMsg.description = "Succeeded to issue the coupon";
        }
        else {
            resultMsg.status = "FAILED";
            resultMsg.description = "Failed to issue the coupon: " + msg.cause;
        }

        transaction.msg = resultMsg;
    }

    transaction.moveToEnd();
}

// It's Global Function !

function dateToString(d) {
    return d.getFullYear()+"-"+pad0(d.getMonth()+1)+"-"+pad0(d.getDate())+" "+pad0(d.getHours())+":"+pad0(d.getMinutes())+":"+pad0(d.getSeconds());
}

function pad0(n) {
    return n>9 ? ""+n : "0"+n
}
