import * as fs from 'fs';
import dotenv from "dotenv";

export async function WriteToXML(json, outputPath, name) {
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