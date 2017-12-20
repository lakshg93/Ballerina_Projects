import ballerina.lang.system;



function main (string[] args) {
    string g="25";
    var val, _=<int>g;
    system:println(val);
    int f = val;
    system:println(f);
}
