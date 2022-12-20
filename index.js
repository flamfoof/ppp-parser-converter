import * as fs from "fs";
import dotenv from "dotenv";
import fetch from "node-fetch";
import {Command} from "commander";
import {dirname, resolve} from "path";
import logbuffer from "console-buffer";
import JSONStream from "JSONStream";
// import pkg from 'csv-parse';
import * as csv from 'csv-parse';
import * as path from 'path';

const program = new Command();


program
	.name("ppp-parser-converter")
	.description(
		"This is a CLI tool to convert and parse data from ppp."
	)
	.version("0.0.1");

program
	.option("-i, --input <path>", "The input source file")
    .option("-d, --download <params>", "The download params (all/#)")
    .option("-o, --output <path>", "The output file path")
    .option("-c, --convert <params>", "The convert params (all/#)")

program.parse(process.argv);

const options = program.opts();
if (!process.argv[2]) {
	console.log("Type -h or --help for available commands");
}

console.log(program.opts());

Init();

async function Init() {
	dotenv.config();
    
    console.log("starting");
    if(!options.input)
    {
        console.log("Input file not specified");
        return;
    }
    var results = [];
    //sloppy quick implementation

var count = 0;
    var req = fs.createReadStream(options.input)
    .pipe(csv.parse())
    .on('data', (data) => {
        var template = {
            "BorrowerName": data[4],
            "BorrowerAddress": data[5],
            "BorrowerCity": data[6],
            "BorrowerState": data[7],
            "BorrowerZip": data[8],
            "LoanStatusDate": data[9],
            "LoanStatus": data[10],
            "Term": data[11],
            "InitialApprovalAmount": data[13],
            "CurrentApprovalAmount": data[14],
            "FranchiseName": data[16],
            "JobsReported": data[32],
            "NAICSCode": data[33],
            "BusinessType": data[43],
            "ForgivenessAmount": data[51],
            "ForgivenessDate": data[52]
        }
        
        Object.keys(template).forEach((el) => { 
            {
                if(template[el].includes(","))
                {
                    template[el] = `"${template[el]}"`;
                }
            }
        });

        count++;
        
        results.push(template)

        if(count % 100000 == 0)
        {
            console.log(count);
        }

        // console.log(results)
        
        // if(count == 2)
        // req.pause();

        logbuffer.flush();
    })
    .on('end', async () => {
        console.log("Writing File....")
        logbuffer.flush();
        await WriteToXML(results, options.output, path.basename(options.input));
    });
}


async function WriteToXML(json, outputPath, name) {
	if (!fs.existsSync(outputPath)) {
		fs.mkdirSync(outputPath, {
			recursive: true,
		});
	}

    // console.log(json)
	var destination = name ? outputPath + "/" + name : outputPath + "/out.csv";

	var xmlData = json.map((el) => {
        return Object.values(el).join(",");
    }).join("\n");

	fs.writeFile(destination, xmlData, "utf8", function (err) {
		if (err) {
			console.log("An error occured while writing JSON Object to File.");
			console.log(err);
			return;
		}
        
		console.log("File has been saved at: " + destination);
	});
}