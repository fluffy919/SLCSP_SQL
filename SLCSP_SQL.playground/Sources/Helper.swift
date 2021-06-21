import Foundation

let homeworkDirectoryUrl = try? FileManager.default.url(for: .documentDirectory, in: .userDomainMask, appropriateFor: nil, create: true)
public let output = URL(fileURLWithPath: "slcsp", relativeTo: homeworkDirectoryUrl).appendingPathExtension("csv")

private enum Database: String {
    case Homework
  
    var path: String? {
        return homeworkDirectoryUrl?.appendingPathComponent("\(self.rawValue).sqlite").relativePath
    }
}

public let homeworkDbPath = Database.Homework.path

private func destroyDatabase(db: Database) {
    guard let path = db.path else { return }
    do {
        if FileManager.default.fileExists(atPath: path) {
            try FileManager.default.removeItem(atPath: path)
        }
    } catch {
        print("Could not destroy \(db) Database file.")
    }
}

public func destroyHWDatabase() {
    destroyDatabase(db: .Homework)
}
