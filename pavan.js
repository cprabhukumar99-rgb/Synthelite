function transform(line) {

  if (line.startsWith("sno")) {
    return "";
  }

  var values = line.split(",");

  return JSON.stringify({
    emp_no: Number(values[0]),
    emp_name: values[1],
    salary: Number(values[2]),
    company: values[3]
  });
}