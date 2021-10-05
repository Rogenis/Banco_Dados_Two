#include "../headers/records.hpp"

// Converte uma variavel date para string
string date_to_str(date d){
	string s = "";
	s += to_string(d.year);
	s += "-";
	s += to_string(d.month);
	s += "-";
	s += to_string(d.day);

	s += " ";
	
	s += to_string(d.hour);
	s += ":";
	s += to_string(d.min);
	s += ":";
	s += to_string(d.sec);

	return s;
}

// a funcao retorna um timestamp 
date str_to_date(string s){

	smatch sm;
	regex timestamp_re("[0-9]{4}-[0-9]{2}-[0-9]{2} [0-9]{2}:[0-9]{2}:[0-9]{2}");

	if(!regex_match(s, sm, timestamp_re)){
		s = "0000-00-00 00:00:00";
	}

	string year = s.substr(0,4);
	string month = s.substr(5,2);
	string day = s.substr(8,2);

	string hour = s.substr(11,2);
	string min = s.substr(14,2);
	string sec = s.substr(17,2);

	date d;

	d.year =  stoi(year, nullptr, 10);
	d.month =  stoi(month, nullptr, 10);
	d.day = stoi(day, nullptr, 10);

	d.hour = stoi(hour, nullptr, 10);
	d.min = stoi(min, nullptr, 10);
	d.sec = stoi(sec, nullptr, 10);

	return d;

}

// Imprime os dados de um registro na saida padrao
void print_record(record* regster){
	cout << "ID: " << regster->id << endl;
	cout << "Title: " << regster->title << endl;
	cout << "Year: " << regster->year << endl;
	cout << "Autors: " << regster->autors << endl;
	cout << "Mention: " << regster->mention << endl;
	cout << "Timestamp: " << date_to_str(regster->timestamp) << endl;
	cout << "Snippet: " << regster->snippet << endl << endl;
}
