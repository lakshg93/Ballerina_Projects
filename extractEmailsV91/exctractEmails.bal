import org.wso2.ballerina.connectors.gmail;
import ballerina.lang.jsons;
import ballerina.lang.messages;
import ballerina.lang.system;
import ballerina.lang.errors;
import ballerina.data.sql;
import ballerina.lang.strings;
import ballerina.lang.datatables;


struct Date {
    int id;
    string date;
}


function main (string[] args) {

    //gmail connector
    string userId = "lakshitha@wso2.com";
    string accessToken = "ya29.GluLBK4qLN3eUZOSrrGXM5qvvgsKAJ-hpaMaMhN61Vr2GjNpSHf58Ho-w4i83yqUGCwY0kvLI1vycLLJQVewVTjVZaAofLDStMS7PlyeLXj0WoXhByfUVe6CxYBC";
    string refreshToken = "1/aZf-g1LXtH-o5yRyFVy4QzOL-FMqG8xRjnkDGNi9Umg";
    string clientId = "463242142268-siajs1d6kqvi4n451obtp77oa8fn74dh.apps.googleusercontent.com";
    string clientSecret = "LJiZIswevXNhW87mgnTU3fGg";
    gmail:ClientConnector gmailConnector = create gmail:ClientConnector(userId, accessToken, refreshToken, clientId, clientSecret);


    //database connector
    string dbURL = "jdbc:mysql://127.0.0.1:3306/Vacations";
    string userName = "root";
    string password = "admin";
    map propertiesMap = {"jdbcUrl":dbURL, "username":userName, "password":password};
    sql:ClientConnector dbConnector = create sql:ClientConnector(propertiesMap);




    sql:Parameter[] param = [];
    datatable lastEntryDataTable = sql:ClientConnector.select(dbConnector, "SELECT * FROM `last_entry`", param);

    string lastEntry;

    while (datatables:hasNext(lastEntryDataTable)) {
        any lastEntryStruct = datatables:next(lastEntryDataTable);
        var lastEntryVariable, _ = (Date)lastEntryStruct;
        lastEntry=lastEntryVariable.date;
    }

    //list threads
    message listThreadResponse;


    int maxResults = 500;
    string labelId = "null";
    string includeSpamTrash = "false";
    string nextPageToken = "";
    string query = "to:vacation-group@wso2.com ";
    system:println(query);

    string lastEntryDate=lastEntry;
    system:println(lastEntryDate);

    boolean endOfPages = false;
    while(!endOfPages) {

        listThreadResponse = gmail:ClientConnector.listThreads(gmailConnector, includeSpamTrash, labelId, <string>maxResults, nextPageToken, query);
        //system:println(listThreadResponse);
        json listThreadResponseJSON = messages:getJsonPayload(listThreadResponse);

        int resultSizeEstimate = jsons:getInt(listThreadResponseJSON, "$.resultSizeEstimate");

        if(resultSizeEstimate<maxResults){
            endOfPages = true;
            system:println(resultSizeEstimate);
        }
        else {
            nextPageToken = jsons:getString(listThreadResponseJSON, "$.nextPageToken");
            system:println(nextPageToken);

        }


        string email;
        int i = 0;
        while (i < maxResults && i<resultSizeEstimate) {
            system:print(i);
            try {
                string argument = "$.threads[" + <string>i + "].id";
                string threadID = jsons:getString(listThreadResponseJSON, argument);
                string threadFormat = "metadata";  //full, metadata, minimal,, raw
                string metaDataHeader = "";

                message gmailReadMailResponseMessage = gmail:ClientConnector.readMail(gmailConnector, threadID, threadFormat, metaDataHeader);
                json gmailReadMailResponseJson = messages:getJsonPayload(gmailReadMailResponseMessage);


                string subject = jsons:toString(jsons:getJson(gmailReadMailResponseJson, "$.payload.headers.[?(@.name=='Subject')].value"));
                string emailsAddress = jsons:toString(jsons:getJson(gmailReadMailResponseJson, "$.payload.headers.[?(@.name=='From')].value"));
                string date = jsons:toString(jsons:getJson(gmailReadMailResponseJson, "$.payload.headers.[?(@.name=='Date')].value"));



                email = extractEmailAddress(emailsAddress); //Extract Email address
                string leaveType = extractLeaveType(subject);    //Extract Subject
                string dateStr = extractDate(date);  //Extract Date

                system:println(dateStr);

                lastEntryDate = getNextLastEntry(dateStr,lastEntryDate);
                system:println(lastEntryDate);

                if (dateStr == lastEntry) {
                    endOfPages = true;
                    break;
                }

                json outputDetails = {"Email":email, "LeaveTaken":leaveType, "leaveSubject":subject, "Data_Applied":dateStr};

                sql:Parameter[] params = [];

                params[0] = {sqlType:"VARCHAR", value:email, direction:0};
                params[1] = {sqlType:"VARCHAR", value:leaveType, direction:0};
                params[2] = {sqlType:"VARCHAR", value:subject, direction:0};
                params[3] = {sqlType:"VARCHAR", value:dateStr, direction:0};


                int insertSuccess = sql:ClientConnector.update(dbConnector, "INSERT INTO vacationData (Email,LeaveTaken,leaveSubject,Data_Applied) VALUES (?,?,?,?)", params);

                boolean errorInInsert = false;
                if (insertSuccess == 0) {
                    errorInInsert = true;
                }
            }
            catch (errors:Error e) {

                system:println("Error in" + i + " mail after:" + email);
            }

            i = i + 1;

        }
    }

    json lastEntryJson = {"date":lastEntryDate};
    sql:Parameter[] params = [];

    params[0] = {sqlType:"VARCHAR", value:lastEntryDate, direction:0};

    int changedLastEntry = sql:ClientConnector.update(dbConnector,"UPDATE `last_entry` SET `date`=? WHERE `id` =1",params);


}

function extractLeaveType(string subject)(string){
    string leaveType;
    subject = strings:toUpperCase(subject);

    if (strings:contains(subject, "OOO")) { leaveType = "OOO";}
    else if (strings:contains(subject, "WFH")) {leaveType = "WFH";}
    else if (strings:contains(subject, "LTO")) {leaveType = "LTO";}
    else if (strings:contains(subject, "EARLY")) {leaveType = "LEAVING EARLY";}
    else if (strings:contains(subject, "HALF DAY")) {leaveType = "HALF DAY";}
    else if (strings:contains(subject, "SICK")) {leaveType = "SICK LEAVE";}
    else if (strings:contains(subject, "ANNUAL")) {leaveType = "ANNUAL LEAVE";}
    else if (strings:contains(subject, "LEAVE")) {leaveType = "ON LEAVE";}
                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                                 else {leaveType = "CAN'T DECIDE";}
    return leaveType;
}

function extractDate(string dateStr)(string){

    string month;

    string[]dateSplitArray = strings:split(dateStr, " ");
    if(dateSplitArray[2]=="Jan"){month ="01";}
    else if(dateSplitArray[2]=="Feb"){month ="02";}
    else if(dateSplitArray[2]=="Mar"){month ="03";}
    else if(dateSplitArray[2]=="Apr"){month ="04";}
    else if(dateSplitArray[2]=="May"){month ="05";}
    else if(dateSplitArray[2]=="Jun"){month ="06";}
    else if(dateSplitArray[2]=="Jul"){month ="07";}
    else if(dateSplitArray[2]=="Aug"){month ="08";}
    else if(dateSplitArray[2]=="Sep"){month ="09";}
    else if(dateSplitArray[2]=="Oct"){month ="010";}
    else if(dateSplitArray[2]=="Nov"){month ="011";}
    else if(dateSplitArray[2]=="Dec"){month ="012";}

    dateStr = dateSplitArray[1] +"/"+month+"/"+dateSplitArray[3];

    return dateStr;
}

function extractEmailAddress(string emailsAddress)(string){
    string[] array = strings:split(emailsAddress, " ");
    int lastIndex = strings:indexOf(array[array.length - 1], ">");
    string email = strings:subString(array[array.length - 1], 1, lastIndex);

    return email;
}

function getNextLastEntry(string date,string currentLastEntry)(string){

    int[]dateArray = getDateinInt(date);
    int[]currentLastEntryArray = getDateinInt(currentLastEntry);
    string lastEntryStr = currentLastEntry;

    if (dateArray[2]>currentLastEntryArray[2]){ // first compare Year
        lastEntryStr = date;
    }
    else if(dateArray[1]>currentLastEntryArray[1]){ // Compare Month
        lastEntryStr = date;
    }
    else if (dateArray[0]>currentLastEntryArray[0]){ // Compare Date
        lastEntryStr = date;
    }

    return lastEntryStr;

}

function getDateinInt(string dateStr)(int[]){
    string[]dateArray = strings:split(dateStr,"/");

    int[]date =[3];
    var val, _=<int>dateArray[0];
     date[0] = val;
     val, _=<int>dateArray[1];
     date[1] = val;
     val, _=<int>dateArray[2];
     date[2] = val;

    return date;
}






