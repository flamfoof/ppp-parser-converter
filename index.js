import * as fs from "fs";
import dotenv from "dotenv";
import {Command} from "commander";
import {dirname, resolve} from "path";
import * as path from 'path';
import logbuffer from "console-buffer";

const program = new Command();


program
	.name("ppp-parser-converter")
	.description(
		"This is a CLI tool to convert and parse data from ppp."
	)
	.version("0.0.1");

program
	.option("-i, --input <path>", "The input source file")
    .option("-naics, --naicsList <path>", "Get the list of NAICS data from a file")
    .option("-d, --Download <params>", "The download params (all/#) TODO...")
    .option("-o, --output <path>", "The output file path")
    .option("-c, --Convert", "The convert params (all/#) TODO...")
    .option("-p, --Package", "The package all the ppp .csv files into one")
    .option("-u, --Upload <params>", "Upload the file(s) to the Dynamo DB")

program.parse(process.argv);

const options = program.opts();
if (!process.argv[2]) {
	console.log("Type -h or --help for available commands");
}

console.log(program.opts());

Init();

async function Init() {
	dotenv.config();
    var keyList = [];
    
    Object.keys(options).forEach( opts => {
        process.env[opts] = options[opts];
        keyList.push(opts);
    })

    const utilFolderItems = fs.readdirSync(resolve(path.basename(dirname("")), "src"), {withFileTypes: true})
        .filter((dirent) => dirent.isFile()).map((dirent) => dirent.name.replace(".js", ""));

    console.log("starting");

    if(!options.input)
    {
        console.log("Input file not specified");
        return;
    }

    for (var i = 0; i < keyList.length; i++) {
		if (utilFolderItems.includes(keyList[i])) {
			console.log("Found function for: " + keyList[i]);
			return await UtilFunction(keyList[i]);
		}
	}
}

async function UtilFunction(fileAction) {
    const util = await import(`./src/${fileAction}.js`);
    return await util.Init();
}