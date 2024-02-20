db = db.getSiblingDB('admin');
rs.initiate({ _id: "${rsId}", configsvr: ${configServer?c}, members: [${members}] });
let isPrimary = false;
let count = 0;
// wait for primary node election
while(isPrimary == false && count < 120) {
  const rplStatus = db.adminCommand({ replSetGetStatus : 1 });
  isPrimary = rplStatus.members[0].stateStr === "PRIMARY";
  print("is primary result: ", isPrimary);
  count = count + 1;
  sleep(1000);
}
