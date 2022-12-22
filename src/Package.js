import fs from 'fs';
import * as csv from 'csv-parse';
import logbuffer from "console-buffer";

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
        
        console.log("Piping: " + filesList[fileCount]);
        
        var reqWrite = fs.createReadStream(process.env.input + "/" + filesList[fileCount])
        reqWrite
            .pipe(csv.parse())
            .on('data', (chunk) => {
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
                
                if(canPush)
                    outputStream.write(xmlData);

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