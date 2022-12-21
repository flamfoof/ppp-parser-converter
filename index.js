import * as fs from "fs";
import dotenv from "dotenv";
import fetch from "node-fetch";
import {Command} from "commander";
import {dirname, resolve} from "path";
import logbuffer from "console-buffer";
import JSONStream from "JSONStream";
// import pkg from 'csv-parse';
import * as csv from 'csv-parse';
import pkg from "csv-parser";
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
    .option("-naics, --naicsList <path>", "Get the list of NAICS data")
    .option("-d, --download <params>", "The download params (all/#)")
    .option("-o, --output <path>", "The output file path")
    .option("-c, --convert <params>", "The convert params (all/#)")
    .option("-p, --package", "The package params (all/#)")

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

    if(options.package)
    {
        await PackageAllCSVIntoOne();
    } else {
        await ParseConvert();
    }
    
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

async function ParseConvert() {
    var naicsListCode = [];
    var naicsListTitle = [];
    var results = [];
    var count = 0;
    var first = true;

    if(options.naicsList)
    {
        fs.createReadStream(options.naicsList)
        .pipe(pkg())
        .on('data', (data) => {
            naicsListCode.push(data.Code);
            naicsListTitle.push(data.Title);
        })
        .on('end', () => {
            console.log("Completed reading Naics List");
            req.resume();
        });
    }
    

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
            "Industry": "Industry",
            "BusinessType": data[43],
            "ForgivenessAmount": data[51],
            "ForgivenessDate": data[52]
        }
        
        if(options.naicsList && naicsListCode.indexOf(template.NAICSCode) != -1)
        {
            if(first)
            {
                first=false;
            } else {
                template.Industry = naicsListTitle[naicsListCode.indexOf(template.NAICSCode)];
            }
        } else {
            template.Industry = "";
        }
        
        Object.keys(template).forEach((el) => { 
            {
                if(template[el].includes(","))
                {
                    template[el] = `"${template[el]}"`;
                } else if (template[el].includes("\""))
                {
                    template[el] = template[el].replaceAll("\"", "");
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
    
    if(options.naicsList)
        req.pause();
}

async function PackageAllCSVIntoOne()
{
    var outputPath = options.output;
    var name;
    var destination = name ? outputPath + "/" + name : outputPath + "/out.csv";
    var filesList = [];
    var fileCount = 0;
    var count = 0;
    console.log("Packiang")
    
    if (!fs.existsSync(outputPath)) {
		fs.mkdirSync(outputPath, {
			recursive: true,
		});
	}

    var outputStream = fs.createWriteStream(destination);

    outputStream.on('error', function (err) {
        console.log(err);
    });

    fs.readdirSync(options.input).forEach(file => filesList.push(file))

    if(filesList.length == 0)
    {
        console.log("No files found in the input folder");
        return;
    }
    
    var inputStream = fs.createReadStream(options.input)
    inputStream
        .on('error', function (err) {
            console.log(err);
        })

    await CreateNewPipe(fileCount)

    async function CreateNewPipe(fileCount)
    {
        if(fileCount > filesList.length)
        {
            return;
        }
        
        var req = fs.createReadStream(options.input + "/" + filesList[fileCount])
        req
            .pipe(csv.parse())
            .on('data', (chunk) => { 
                console.log(count++);

                var xmlData = chunk.map((el) => {
                    return Object.values(el).join(",");
                }).join("\n");
                outputStream.write(xmlData);
            })
            .on("end", () => {
                console.log("Done");
                fileCount++;
                CreateNewPipe(fileCount);
            })
            .on("error", (err) => {
                console.log(err);
            })
    }
}