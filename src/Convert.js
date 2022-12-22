import logbuffer from "console-buffer";
import * as fs from "fs";
import * as csv from 'csv-parse';
import pkg from "csv-parser";
import * as FilesUtil from './util/Files.js';
import * as path from "path";

export async function Init()
{
    return await ParseConvert();
}

export async function ParseConvert() {
    var naicsListCode = [];
    var naicsListTitle = [];
    var results = [];
    var count = 0;
    var first = true;
    console.log("default")

    if(process.env.naicsList)
    {
        fs.createReadStream(process.env.naicsList)
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
    

    var req = fs.createReadStream(process.env.input)
    .pipe(csv.parse())
    .on('data', (data) => {
        var template = {
            "LoanNumber": data[0],
            "DateApproved": data[1],
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
        
        if(process.env.naicsList && naicsListCode.indexOf(template.NAICSCode) != -1)
        {
            if(first)
            {
                first=false;
            } else {
                template.Industry = naicsListTitle[naicsListCode.indexOf(template.NAICSCode)];
            }
        } else {
            if(first)
            {
                first=false;
            } else {
                template.Industry = "";
            }
        }
        
        Object.keys(template).forEach((el) => { 
            {
                if (template[el].includes("\""))
                {
                    template[el] = template[el].replaceAll("\"", "");
                }
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
        await FilesUtil.WriteToXML(results, process.env.output, path.basename(process.env.input));
    });
    
    if(process.env.naicsList)
        req.pause();
}