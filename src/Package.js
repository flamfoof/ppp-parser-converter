import fs from 'fs';
import * as csv from 'csv-parse';
import logbuffer from "console-buffer";
import path from "path";

export async function Init()
{
    return await PackageAllCSVIntoOne();
}

export async function PackageAllCSVIntoOne()
{
    var outputPath = process.env.output;
    var name;
    var destination = name ? outputPath + "/" + name : outputPath + "/out.csv";
    var filesList = [];
    var fileCount = 0;
    var count = 0;
    var currData;
    var first = true;
    var canPush = false;
    var prevState = "";
    var defaultHeaderData = "LoanNumber,DateApproved,BorrowerName,BorrowerAddress,BorrowerCity,BorrowerState,BorrowerZip,LoanStatusDate,LoanStatus,Term,InitialApprovalAmount,CurrentApprovalAmount,FranchiseName,JobsReported,NAICSCode,Industry,BusinessType,ForgivenessAmount,ForgivenessDate";
    
    if (!fs.existsSync(outputPath)) {
		fs.mkdirSync(outputPath, {
			recursive: true,
		});
	}

    var outputStream = fs.createWriteStream(destination);

    outputStream.on('error', function (err) {
        console.log(err);
    });

    fs.readdirSync(process.env.input).forEach(file => filesList.push(file))

    if(filesList.length == 0)
    {
        console.log("No files found in the input folder");
        return;
    }

    console.log(filesList[fileCount]);
    
    var inputStream = fs.createReadStream(process.env.input)
    inputStream
        .on('error', function (err) {
            console.log(err);
        })

    CreateNewPipe(fileCount)

    async function CreateNewPipe(fileCount)
    {
        if(fileCount >= filesList.length)
        {
            console.log("Reached the end of the files")
            return;
        }
        var fileTarget = process.env.input + "/" + filesList[fileCount]
        console.log("Piping: " + filesList[fileCount]);
        
        var reqWrite = fs.createReadStream(fileTarget)
        reqWrite
            .pipe(csv.parse())
            .on('data', (chunk) => {
                var finalFileDestination = "";
                var stateLocation = "";
                count++;
                currData = chunk;

                //clean up the texts
                chunk.forEach((el) => { 
                    if (el.includes("\""))
                    {
                        chunk[chunk.indexOf(el)] = el.replaceAll("\"", "");
                    }
                    if(el.includes(","))
                    {
                        chunk[chunk.indexOf(el)] = `"${el}"`;
                    }
                });

                if(count == 1 && first)
                {
                    first = false;
                    canPush = true;
                } else if(count != 1)
                {
                    canPush = true;
                }
                
                var xmlData = chunk.map((el) => {
                    return el;
                }).join(",") + "\n";

                stateLocation = chunk[5]

                if(stateLocation == "")
                {
                    stateLocation = "other";
                }
                
                finalFileDestination = outputPath + "/" + path.basename(fileTarget).replace(path.extname(fileTarget), "") + "_" + stateLocation + path.extname(fileTarget);
                // console.log(finalFileDestination);
                // if(count == 100)
                //     process.exit(1);
                
                if(canPush)
                {
                    // outputStream.write(xmlData);

                    if(chunk[5] != "BorrowerState")
                    {
                        if(!fs.existsSync(finalFileDestination))
                        {
                            fs.appendFileSync(finalFileDestination, defaultHeaderData, (err) => {
                                console.log(err);
                                process.exit(1);
                            });
                        }

                        fs.appendFileSync(finalFileDestination, xmlData, (err) => {
                            console.log(err);
                            process.exit(1);
                        });
                    }
                }
                if(count % 100000 == 0)
                {
                    console.log(count);
                }
                
                // if(count >= 10)
                // {
                //     console.log("pausing")
                //     reqWrite.pause();
                //     // outputStream.end();
                //     process.exit(1)
                // }

                logbuffer.flush();
            })
            .on("end", () => {
                console.log("Done");
                fileCount++;
                count = 0;
                CreateNewPipe(fileCount);
            })
            .on("error", (err) => {
                console.log("Error on index: " + count)
                console.log(currData)
                console.log(err);
            })
    }
}