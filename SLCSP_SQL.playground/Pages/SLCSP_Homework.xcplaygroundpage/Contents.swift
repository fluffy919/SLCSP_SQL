import Foundation
import SQLite3
import PlaygroundSupport

// Open CSV files
func openCSV(fileName: String, fileType: String) -> String!{
    guard let filepath = Bundle.main.path(forResource: fileName, ofType: fileType)
        else {
            return nil
    }
    
    do {
        let contents = try String(contentsOfFile: filepath, encoding: .utf8)
        return contents
    } catch {
        print("File Read Error for file \(filepath)")
        return nil
    }
}


func parseCSV(fileName: String) -> [String] {
    let data: String! = openCSV(fileName: fileName, fileType: "csv")
    let rows = data.components(separatedBy: "\n")
    
    return rows
}

var plans = parseCSV(fileName: "plans")
var zips = parseCSV(fileName: "zips")
var slcsp = parseCSV(fileName: "slcsp")
plans.removeFirst() // remove header
zips.removeFirst() // remove header
slcsp.removeFirst() // remove header

// SQL
destroyHWDatabase()

enum SQLiteError: Error {
    case OpenDatabase(message: String)
    case Prepare(message: String)
    case Step(message: String)
    case Bind(message: String)
}

// The Database Connection
class SQLiteDatabase {
    private let dbPointer: OpaquePointer?
    private init(dbPointer: OpaquePointer?) {
        self.dbPointer = dbPointer
    }
  
    fileprivate var errorMessage: String {
        if let errorPointer = sqlite3_errmsg(dbPointer) {
            let errorMessage = String(cString: errorPointer)
            return errorMessage
        } else {
            return "No error message provided from sqlite."
        }
    }
  
    deinit {
        sqlite3_close(dbPointer)
    }
  
    static func open(path: String) throws -> SQLiteDatabase {
        var db: OpaquePointer?
    
        if sqlite3_open(path, &db) == SQLITE_OK {
            return SQLiteDatabase(dbPointer: db)
        } else {
            defer {
                if db != nil {
                    sqlite3_close(db)
                }
            }
      
            if let errorPointer = sqlite3_errmsg(db) {
                let message = String(cString: errorPointer)
                throw SQLiteError.OpenDatabase(message: message)
            } else {
                throw SQLiteError.OpenDatabase(message: "No error message provided from sqlite.")
            }
        }
    }
}

let db: SQLiteDatabase

do {
    db = try SQLiteDatabase.open(path: homeworkDbPath!)
    print("Successfully opened connection to database.")
} catch SQLiteError.OpenDatabase(_) {
    print("Unable to open database.")
    PlaygroundPage.current.finishExecution()
}

// Preparing Statements
extension SQLiteDatabase {
    func prepareStatement(sql: String) throws -> OpaquePointer? {
        var statement: OpaquePointer?
        guard sqlite3_prepare_v2(dbPointer, sql, -1, &statement, nil) == SQLITE_OK else {
            throw SQLiteError.Prepare(message: errorMessage)
        }
        return statement
    }
}

// Create Table
struct Plan {
    let id: Int32
    let plan_id: NSString
    let state: NSString
    let metal_level: NSString
    let rate: Double
    let rate_area: Int32
}

struct Zip {
    let id: Int32
    let zipcode: NSString
    let state: NSString
    let county_code: NSString
    let name: NSString
    let rate_area: Int32
}

struct SLCSP {
    let id: Int32
    let zipcode: NSString
}

protocol SQLTable {
    static var createStatement: String { get }
}

extension Plan: SQLTable {
    static var createStatement: String {
        return """
            CREATE TABLE IF NOT EXISTS Plans(
            Id INT PRIMARY KEY NOT NULL,
            plan_id CHAR(15),
            state CHAR(5),
            metal_level CHAR(20),
            rate DOUBLE,
            rate_area INT);
            """
    }
}

extension Zip: SQLTable {
    static var createStatement: String {
        return """
            CREATE TABLE IF NOT EXISTS Zips(
            Id INT PRIMARY KEY NOT NULL,
            zipcode CHAR(5),
            state CHAR(5),
            county_code CHAR(10),
            name CHAR(100),
            rate_area INT);
            """
    }
}

extension SLCSP: SQLTable {
    static var createStatement: String {
        return """
            CREATE TABLE IF NOT EXISTS SLCSP(
            Id INT PRIMARY KEY NOT NULL,
            zipcode CHAR(5));
            """
    }
}

extension SQLiteDatabase {
    func createTable(table: SQLTable.Type) throws {
        let createTableStatement = try prepareStatement(sql: table.createStatement)

        defer {
            sqlite3_finalize(createTableStatement)
        }

        guard sqlite3_step(createTableStatement) == SQLITE_DONE else {
            throw SQLiteError.Step(message: errorMessage)
        }
        
        print("\(table) table created.")
    }
}

do {
    try db.createTable(table: Plan.self)
    try db.createTable(table: Zip.self)
    try db.createTable(table: SLCSP.self)
} catch {
    print(db.errorMessage)
}

// Insert
extension SQLiteDatabase {
    func insertPlanRow(plan: Plan) throws {
        let insertSql = "INSERT INTO Plans (Id, plan_id, state, metal_level, rate, rate_area) VALUES (?, ?, ?, ?, ?, ?);"
        let insertStatement = try prepareStatement(sql: insertSql)
        
        defer {
            sqlite3_finalize(insertStatement)
        }
        
        let plan_id: NSString = plan.plan_id
        let state: NSString = plan.state
        let metal_level: NSString = plan.metal_level
        let rate: Double = plan.rate
        let rate_area: Int32 = plan.rate_area
        
        guard
            sqlite3_bind_int(insertStatement, 1, plan.id) == SQLITE_OK  &&
            sqlite3_bind_text(insertStatement, 2, plan_id.utf8String, -1, nil) == SQLITE_OK &&
            sqlite3_bind_text(insertStatement, 3, state.utf8String, -1, nil) == SQLITE_OK &&
            sqlite3_bind_text(insertStatement, 4, metal_level.utf8String, -1, nil) == SQLITE_OK &&
            sqlite3_bind_double(insertStatement, 5, rate) == SQLITE_OK &&
            sqlite3_bind_int(insertStatement, 6, rate_area) == SQLITE_OK
        else {
            throw SQLiteError.Bind(message: errorMessage)
        }
    
        guard sqlite3_step(insertStatement) == SQLITE_DONE else {
            throw SQLiteError.Step(message: errorMessage)
        }
    }
    
    func insertZipRow(zip: Zip) throws {
        let insertSql = "INSERT INTO Zips (Id, zipcode, state, county_code, name, rate_area) VALUES (?, ?, ?, ?, ?, ?);"
        let insertStatement = try prepareStatement(sql: insertSql)
        
        defer {
            sqlite3_finalize(insertStatement)
        }
        
        let zipcode: NSString = zip.zipcode
        let state: NSString = zip.state
        let county_code: NSString = zip.county_code
        let name: NSString = zip.name
        let rate_area: Int32 = zip.rate_area
        
        guard
            sqlite3_bind_int(insertStatement, 1, zip.id) == SQLITE_OK  &&
            sqlite3_bind_text(insertStatement, 2, zipcode.utf8String, -1, nil) == SQLITE_OK &&
            sqlite3_bind_text(insertStatement, 3, state.utf8String, -1, nil) == SQLITE_OK &&
            sqlite3_bind_text(insertStatement, 4, county_code.utf8String, -1, nil) == SQLITE_OK &&
            sqlite3_bind_text(insertStatement, 5, name.utf8String, -1, nil) == SQLITE_OK &&
            sqlite3_bind_int(insertStatement, 6, rate_area) == SQLITE_OK
        else {
            throw SQLiteError.Bind(message: errorMessage)
        }
    
        guard sqlite3_step(insertStatement) == SQLITE_DONE else {
            throw SQLiteError.Step(message: errorMessage)
        }
    }
    
    func insertSLCSPRow(slcsp: SLCSP) throws {
        let insertSql = "INSERT INTO SLCSP (Id, zipcode) VALUES (?, ?);"
        let insertStatement = try prepareStatement(sql: insertSql)
        
        defer {
            sqlite3_finalize(insertStatement)
        }
        
        let zipcode: NSString = slcsp.zipcode
      
        guard
            sqlite3_bind_int(insertStatement, 1, slcsp.id) == SQLITE_OK  &&
            sqlite3_bind_text(insertStatement, 2, zipcode.utf8String, -1, nil) == SQLITE_OK
        else {
            throw SQLiteError.Bind(message: errorMessage)
        }
    
        guard sqlite3_step(insertStatement) == SQLITE_DONE else {
            throw SQLiteError.Step(message: errorMessage)
        }
    }
}

func insertZips(stringArr: [String]) {
    var id: Int32 = 1
    
    for row in stringArr {
        if row.isEmpty { // the last EMPTY line of zips file
            continue
        }
        
        let zip = row.components(separatedBy: ",")
        
        do {
            try db.insertZipRow(zip: Zip(id: id,
                                      zipcode: zip[0] as NSString,
                                      state: zip[1] as NSString,
                                      county_code: zip[2] as NSString,
                                      name: zip[3] as NSString,
                                      rate_area: Int32(Int(zip[4])!)))
            id += 1
        } catch {
            print(db.errorMessage)
        }
    }
    
    print("Successfully inserted zips rows.")
}

func insertPlans(stringArr: [String]) {
    var id: Int32 = 1
    
    for row in stringArr {
        if row.isEmpty { // the last EMPTY line of plans file
            continue
        }
        
        let plan = row.components(separatedBy: ",")
        
        do {
            try db.insertPlanRow(plan: Plan(id: id,
                                         plan_id: plan[0] as NSString,
                                         state: plan[1] as NSString,
                                         metal_level: plan[2] as NSString,
                                         rate: Double(plan[3])!,
                                         rate_area: Int32(Int(plan[4])!)))
            id += 1
        } catch {
            print(db.errorMessage)
        }
    }
    
    print("Successfully inserted plans rows.")
}

func insertSLCSP(stringArr: [String]) {
    var id: Int32 = 1
    
    for row in stringArr {
        if row.isEmpty { // the last EMPTY line of slcsp file
            continue
        }
        
        let plan = row.components(separatedBy: ",")
        
        do {
            try db.insertSLCSPRow(slcsp: SLCSP(id: id,
                                            zipcode: plan[0] as NSString))
            id += 1
        } catch {
            print(db.errorMessage)
        }
    }
    
    print("Successfully inserted slcsp rows.")
}

insertPlans(stringArr: plans)
insertZips(stringArr: zips)
insertSLCSP(stringArr: slcsp)

// Combine
extension SQLiteDatabase {
    func combine() throws {
        let combineQuery = """
                        CREATE TABLE Combine AS
                        SELECT Plans.state, Plans.metal_level, Plans.rate, Plans.rate_area, Zips.zipcode
                        FROM Plans
                        LEFT JOIN Zips
                        ON (Plans.state = Zips.state
                        AND Plans.rate_area = Zips.rate_area)
                        WHERE Plans.metal_level = "Silver"
                        ORDER BY Zips.zipcode;
                        """
        let combineStatement = try prepareStatement(sql: combineQuery)
        
        defer {
            sqlite3_finalize(combineStatement)
        }
        
        guard sqlite3_step(combineStatement) == SQLITE_DONE else {
            throw SQLiteError.Step(message: errorMessage)
        }
        
        print("Successfully combined tables")
    }
}

do {
    try db.combine()
} catch {
    print(db.errorMessage)
}


// Read & Write
extension SQLiteDatabase {
    func readWithZip() throws {
        
        // Read from SQL
        let querySql = "SELECT zipcode, rate FROM Combine WHERE zipcode In (SELECT zipcode FROM SLCSP);"

        var filteredValue: [String: [Double]] = [:]

        guard let queryStatement = try? prepareStatement(sql: querySql) else {
            return
        }

        defer {
            sqlite3_finalize(queryStatement)
        }
        
        while sqlite3_step(queryStatement) == SQLITE_ROW {
            let queryResultCol0 = sqlite3_column_text(queryStatement, 0)
            if queryResultCol0 == nil { continue }
            let zipcode = String(cString: queryResultCol0!)
            let rate = sqlite3_column_double(queryStatement, 1)

            if let _ = filteredValue[zipcode] {
                filteredValue[zipcode]?.append(rate)
            }
            else {
                filteredValue[zipcode] = [rate]
            }
        }

        var result: [(zip: String, rate: Double)] = []

        // The second lowest value for each zipcode
        for zip in slcsp {
            let zipCode = zip.replacingOccurrences(of: ",", with: "")
            
            if zipCode == "" { continue }
            
            if filteredValue[zipCode] == nil { result += [(zip: zipCode, rate: 0.0)]; continue }
            
            if filteredValue[zipCode]!.count < 2 { result += [(zip: zipCode, rate: 0.0)]; continue }
            
            let lowest = filteredValue[zipCode]?.min()!
            let secondLow = filteredValue[zipCode]!.filter { $0 > lowest! }.min()!
            
            result += [(zip: zipCode, rate: secondLow)]
        }

        // Write a output file
        guard let data = "zipcode,rate\n".data(using: .utf8) else {
            print("Unable to convert value to data")
            return
        }
        
        do {
            try data.write(to: output)
        } catch {
            print(error.localizedDescription)
        }
        
        for line in result {
            let zip: String = line.zip
            var rate: String = ""
            
            if line.rate != 0.0 {
                rate = String(format: "%.2f", line.rate)
            }
            
            let zip_rate = zip + "," + rate
            
            do {
                try zip_rate.appendLine(to: output)
            } catch {
                print(error.localizedDescription)
            }
        }
        
        print("All Done")
    }
}


do {
    try db.readWithZip()
} catch {
    print(db.errorMessage)
}

extension String {
    func appendLine(to url: URL) throws {
        try self.appending("\n").append(to: url)
    }
    
    func append(to url: URL) throws {
        let data = self.data(using: String.Encoding.utf8)
        try data?.append(to: url)
    }
}

extension Data {
    func append(to url: URL) throws {
        if let fileHandle = try? FileHandle(forWritingTo: url) {
            defer {
                fileHandle.closeFile()
            }
            
            fileHandle.seekToEndOfFile()
            fileHandle.write(self)
        }
        else {
            try write(to: url)
        }
    }
}
