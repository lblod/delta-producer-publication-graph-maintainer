import { execSync } from 'child_process';
import * as tmp from 'tmp';
import * as Readlines from '@lazy-node/readlines';
import * as fs from 'fs';

const SHELL_CONFIG = {
  encoding: 'utf-8',
  stdio: ['ignore', 'ignore', 'ignore'], //no output
  shell: '/bin/bash'
};

export function arrayToFile(array, file){
  let fd = file.fd;
  for (let i=0; i<array.length; ++i){
    fs.writeSync(fd, JSON.stringify(array[i]) + "\n");
  }
  return file;
}
/*
 * Takes two files, merges them to a new file
 */
export function mergeFiles(fileA, fileB, cleanAfterMerge = false) {
  let newFile = tmp.fileSync();
  execSync(`cat ${fileA.name} ${fileB.name} | tee ${newFile.name}`, SHELL_CONFIG || {});
  if(cleanAfterMerge) {
    fileA.removeCallback();
    fileB.removeCallback();
  }
  return newFile;
}

// read the file and parse each line to an Object, the opposite of the above function
export function lines(filename) {
  let retval = [];
  let rl = new Readlines(filename);
  let line;
  while ((line = rl.next())) {
    line = line.toString();
    retval.push(JSON.parse(line));
  }
  return retval;
}

export function diffFiles(targetFile, sourceFile, S="50%", T="/tmp"){
  // Note: the S and T parameters can be used to tweak the memory usage of the sort command
  console.log(`Diffing; file based!`);

  let sorted1 = tmp.fileSync();
  let sorted2 = tmp.fileSync();

  execSync(`sort ${targetFile.name} -S ${S} -T ${T} -o ${sorted1.name}`, SHELL_CONFIG);
  execSync(`sort ${sourceFile.name} -S ${S} -T ${T} -o ${sorted2.name}`, SHELL_CONFIG);

  let output1 = tmp.fileSync();
  let output2 = tmp.fileSync();

  execSync(`comm -23 ${sorted1.name} ${sorted2.name} | tee ${output1.name}`, SHELL_CONFIG);
  execSync(`comm -13 ${sorted1.name} ${sorted2.name} | tee ${output2.name}`, SHELL_CONFIG);

  sorted1.removeCallback();
  sorted2.removeCallback();

  return {
    inserts: output1,
    deletes: output2
  };
}
