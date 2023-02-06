use crate::schema::Schema;

pub enum Input {
    Create { table_name: String, schema: Schema },
}

pub fn parse(input_str: &str) -> Input {
    todo!()
}
