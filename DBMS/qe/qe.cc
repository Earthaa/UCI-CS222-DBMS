
#include "qe.h"

Filter::Filter(Iterator *input, const Condition &condition) {
    this->input = input;
    this->condition = condition;
    getAttributes(this->attributes);
}
RC Filter::getNextTuple(void *data) {
    while(input->getNextTuple(data) != QE_EOF){
        if(QEDataHelper::isSatisfy((char*)data,this->attributes,this->condition))
            return 0;
    }
    return QE_EOF;

}


RC Project::getNextTuple(void *data) {

    char* rawTuple = (char*) calloc(PAGE_SIZE, sizeof(char));
    RC rc = input->getNextTuple(rawTuple);
    if(rc != 0){
        free(rawTuple);
        return -1;
    }

    int rawPivot = ceil((double)this->attributes.size() / 8);
    char* rawNullBytes = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(rawNullBytes, rawTuple, rawPivot);

    int newPivot = ceil((double)this->attrNames.size() / 8);
    char* newNullBytes = (char*)calloc(PAGE_SIZE, sizeof(char));
    // index is for the index of the attribute in new tuple
    int index = 0;

    for(int i = 0; i < attributes.size(); i++){
        if(DataHelper::isNull(i,rawNullBytes)){
            if(attrNames[index] == attributes[i].name){
                ++index;
                DataHelper::setNull(index, rawNullBytes);
            }
            continue;
        }
        if(attributes[i].name == attrNames[index]){
            if(attributes[i].type == TypeVarChar){
                int strLength;
                memcpy(&strLength, rawTuple + rawPivot, sizeof(int));
                memcpy((char*)data + newPivot, rawTuple + rawPivot, sizeof(int) + strLength);
                newPivot += (strLength + sizeof(int));
            }
            else{
                memcpy((char*)data + newPivot, rawTuple + rawPivot, sizeof(int));
                newPivot += sizeof(int);
            }
            ++index;
        }
        if(attributes[i].type == TypeVarChar){
            int strLength;
            memcpy(&strLength, rawTuple + rawPivot, sizeof(int));
            rawPivot += (strLength + sizeof(int));
        }
        else{
            rawPivot += sizeof(int);
        }
    }
    // set null bytes
    memcpy(data, newNullBytes, ceil((double)this->attrNames.size() / 8));
    free(newNullBytes);
    free(rawNullBytes);
    free(rawTuple);
    return 0;
}

RC BNLJoin::getNextTuple(void *data){
    char* rightKey = (char*) calloc(PAGE_SIZE, sizeof(char));
    // get the key of the tuple from the right
    JoinHelper::findKey(rightKey, this->currentRightData, this->rightAttrs, this->condition.rhsAttr);
    
    int vectorSize = 0;
    int intKey = 0;
    int strLength = 0;
    float floatKey = 0;
    char* strKey = (char*) calloc(PAGE_SIZE, sizeof(char));
    //get the size of the match tuples in the map
    switch (this->attrType){
        case TypeInt:
            memcpy(&intKey, rightKey, sizeof(int));
            vectorSize = (this->intMap.find(intKey) == this->intMap.end()) ? 0 : this->intMap.at(intKey).size();
            break;
        case TypeReal:
            memcpy(&floatKey, rightKey, sizeof(int));
            vectorSize = (this->floatMap.find(floatKey) == this->floatMap.end()) ? 0 : this->floatMap.at(floatKey).size();
            break;
        case TypeVarChar:
            memcpy(&strLength, rightKey, sizeof(int));
            memcpy(strKey, rightKey + sizeof(int), strLength);
            vectorSize = (this->strMap.find(std::string(strKey)) == this->strMap.end()) ? 0 : this->strMap.at(std::string(strKey)).size();
            break;
    }

    char* leftCharPointer = 0;

    //when the current right tuple can found multiple match in the left
    // currentRightPivot is the next one going to read
    if (this->currentRightPivot < vectorSize){
        switch (this->attrType){
            case TypeInt:
                leftCharPointer = &this->intMap.at(intKey).at(this->currentRightPivot)[0];
                break;
            case TypeReal:
                leftCharPointer = &this->floatMap.at(floatKey).at(this->currentRightPivot)[0];
                break;
            case TypeVarChar:
                leftCharPointer = &this->strMap.at(std::string(strKey)).at(this->currentRightPivot)[0];
                break;
        }
        this->currentRightPivot++;
        // double check if it matches, maybe useless
        if (JoinHelper::isSatisfy(leftCharPointer, this->currentRightData, this->leftAttrs, this->rightAttrs, this->condition)){
            JoinHelper::join((char*)data, leftCharPointer, this->currentRightData, this->leftAttrs, this->rightAttrs);
            free(rightKey);
            free(strKey);
            return 0;
        }
    }

    //when there's still tuple remainning in the right 
    while (this->rightIn->getNextTuple(this->currentRightData) != QE_EOF){
        this->currentRightPivot = 0;
        JoinHelper::findKey(rightKey, this->currentRightData, this->rightAttrs, this->condition.rhsAttr);
        switch (this->attrType){
            case TypeInt:
                memcpy(&intKey, rightKey, sizeof(int));
                vectorSize = (this->intMap.find(intKey) == this->intMap.end()) ? 0 : this->intMap.at(intKey).size();
                break;
            case TypeReal:
                memcpy(&floatKey, rightKey, sizeof(int));
                vectorSize = (this->floatMap.find(floatKey) == this->floatMap.end()) ? 0 : this->floatMap.at(floatKey).size();
                break;
            case TypeVarChar:
                memcpy(&strLength, rightKey, sizeof(int));
                memcpy(strKey, rightKey + sizeof(int), strLength);
                vectorSize = (this->strMap.find(std::string(strKey)) == this->strMap.end()) ? 0 : this->strMap.at(std::string(strKey)).size();
                break;
        }

        //this tuple from the right has matches in the map
        if (this->currentRightPivot < vectorSize){
            char* leftCharPointer = 0;
            switch (this->attrType){
                case TypeInt:
                    leftCharPointer = &this->intMap.at(intKey).at(this->currentRightPivot)[0];
                    break;
                case TypeReal:
                    leftCharPointer = &this->floatMap.at(floatKey).at(this->currentRightPivot)[0];
                    break;
                case TypeVarChar:
                    leftCharPointer = &this->strMap.at(std::string(strKey)).at(this->currentRightPivot)[0];
                    break;
            }
            this->currentRightPivot++;
            // double check if it matches, maybe useless
            if (JoinHelper::isSatisfy(leftCharPointer, this->currentRightData, this->leftAttrs, this->rightAttrs, this->condition)){
                JoinHelper::join((char*)data, leftCharPointer, this->currentRightData, this->leftAttrs, this->rightAttrs);
                free(rightKey);
                free(strKey);
                return 0;
            }
        }
    }
    free(rightKey);
    free(strKey);

    //needs to change the left part in the map
    // and refresh the right part
    if (!updateLeftMap()){
        return QE_EOF;
    }
    this->rightIn->setIterator();
    return getNextTuple(data);
}

//update the left map, return true when successful (has new tuple)
//false when left is to the end
bool BNLJoin::updateLeftMap(){
    this->intMap.clear();
    this->floatMap.clear();
    this->strMap.clear();
    this->remainSize = this->maxSize;

    char* leftData = (char*) calloc(PAGE_SIZE, sizeof(char));
    char* leftKey = (char*) calloc(PAGE_SIZE, sizeof(char));
    int vectorSize = 0;
    int intKey = 0;
    int strLength = 0;
    float floatKey = 0;
    char* strKey = (char*) calloc(PAGE_SIZE, sizeof(char));

    while(hasSpace() && this->leftIn->getNextTuple(leftData) != QE_EOF){
        JoinHelper::findKey(leftKey, leftData, this->leftAttrs, this->condition.lhsAttr);
        int singleSize = getSize(leftData, this->leftAttrs);
        //TODO can I put char* into vector and reuse char*?
        switch (this->attrType){
            case TypeInt:
                memcpy(&intKey, leftKey, sizeof(int));
                if (this->intMap.find(intKey) == this->intMap.end()){
                    this->intMap[intKey] = std::vector<std::vector<char>>{std::vector<char>(leftData, leftData + singleSize)};
                } else {
                    this->intMap.at(intKey).push_back(std::vector<char>(leftData, leftData + singleSize));
                }
                break;
            case TypeReal:
                memcpy(&floatKey, leftKey, sizeof(int));
                if (this->floatMap.find(floatKey) == this->floatMap.end()){
                    this->floatMap[floatKey] = std::vector<std::vector<char>>{std::vector<char>(leftData, leftData + singleSize)};
                } else {
                    this->floatMap.at(floatKey).push_back(std::vector<char>(leftData, leftData + singleSize));
                }
                break;
            case TypeVarChar:
                memcpy(&strLength, leftKey, sizeof(int));
                memcpy(strKey, leftKey + sizeof(int), strLength);
                if (this->strMap.find(std::string(strKey)) == this->strMap.end()){
                    this->strMap[std::string(strKey)] = std::vector<std::vector<char>>{std::vector<char>(leftData, leftData + singleSize)};
                } else {
                    this->strMap.at(std::string(strKey)).push_back(std::vector<char>(leftData, leftData + singleSize));
                }
                break;
        }
        this->remainSize -= singleSize;
    }

    free(leftData);
    free(leftKey);
    free(strKey);
    //the map is full
    if (!hasSpace()) return true;
    //the map is not empty
    if (this->intMap.size() != 0 || this->floatMap.size() != 0 || this->strMap.size() != 0) return true;
    // the map is not updated at all, which means the left is to the end
    return false;
}

int BNLJoin::getSize(char* data, const std::vector<Attribute> &attrs){
    int pivot = ceil((double)attrs.size() / 8);
    char* nullBytes = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(nullBytes, data, pivot);
    int strLength = 0;

    for(int i = 0 ; i < attrs.size(); i++){
        if(DataHelper::isNull(i, nullBytes))
            continue;
        if(attrs[i].type == TypeVarChar){
            memcpy(&strLength, data + pivot, sizeof(int));
            pivot += (sizeof(int) + strLength);
        }
        else{
            pivot += sizeof(int);
        }
    }
    free(nullBytes);
    return pivot;
}

RC INLJoin::getNextTuple(void *data){
    char* rightData = (char*) calloc(PAGE_SIZE, sizeof(char));
    //search through the left (outer) tuple
    while (this->currentLeftStatus != QE_EOF){
        //search through the right (inner) tuple
        while(this->rightIn->getNextTuple(rightData) != QE_EOF){
            if(JoinHelper::isSatisfy(this->currentLeftData, rightData, this->leftAttrs, this->rightAttrs, this->condition)){
                JoinHelper::join((char*) data, this->currentLeftData, rightData, this->leftAttrs, this->rightAttrs);
                free(rightData);
                return 0;
            }
        }
        //when the right (inner) tuple exhausted, read the next tuple from the left and reset the right iterator.
        this->currentLeftStatus = this->leftIn->getNextTuple(this->currentLeftData);
        // this->rightIn->setIterator(NULL, NULL, true, true);
        if (this->currentLeftStatus != QE_EOF){
            char* rightKey = (char*) calloc(PAGE_SIZE, sizeof(char));
            JoinHelper::findKey(rightKey, this->currentLeftData, this->leftAttrs, this->condition.lhsAttr);
            this->rightIn->setIterator(rightKey, rightKey, true, true);
            free(rightKey);
        }
    }
    free(rightData);
    return QE_EOF;
}

void Aggregate::getAttributes(std::vector<Attribute> &attrs) const {
        std::string name;
        switch(this->op){
            case MIN:
                name = "MIN";
                break;
            case MAX:
                name = "MAX";
                break;
            case COUNT:
                name = "COUNT";
                break;
            case SUM:
                name = "SUM";
                break;
            case AVG:
                name = "AVG";
                break;
        }
        std::string attrName = name + "(" + this->aggAttr.name + ")";
        Attribute attr;
        attr.name = attrName;
        attr.type = this->aggAttr.type;
        attr.length = this->aggAttr.length;
        attrs.push_back(attr);
}

RC Aggregate::getNextTuple(void *data){
    //only run one time for single
    if (this->finished) return QE_EOF;

    char* tuple = (char*) calloc(PAGE_SIZE, sizeof(char));
    char* key = (char*) calloc(PAGE_SIZE, sizeof(char));
    int intKey = 0;
    float floatKey = 0;
    std::vector<Attribute> oldAttrs;
    input->getAttributes(oldAttrs);

    // go through the input
    while(input->getNextTuple(tuple) != QE_EOF){
        JoinHelper::findKey(key, tuple, oldAttrs, this->aggAttr.name);
        this->count++;
        switch(op){
            case MAX:
                if (this->aggAttr.type == TypeInt){
                    memcpy(&intKey, key, sizeof(int));
                    this->sum = std::max(this->sum, (float)intKey);
                } else if (this->aggAttr.type == TypeReal){
                    memcpy(&floatKey, key, sizeof(int));
                    this->sum = std::max(this->sum, floatKey);
                }
                break;
            case MIN:
                if (this->aggAttr.type == TypeInt){
                    memcpy(&intKey, key, sizeof(int));
                    this->sum = std::min(this->sum, (float)intKey);
                } else if (this->aggAttr.type == TypeReal){
                    memcpy(&floatKey, key, sizeof(int));
                    this->sum = std::min(this->sum, floatKey);
                }
                break;
            case COUNT:
                // does nothing here because counter is incremented anyway
                break;
            case AVG:
                //then continue with the sum
            case SUM:
                if (this->aggAttr.type == TypeInt){
                    memcpy(&intKey, key, sizeof(int));
                    this->sum += (float)intKey;
                } else if (this->aggAttr.type == TypeReal){
                    memcpy(&floatKey, key, sizeof(int));
                    this->sum += this->sum, floatKey;
                }
                break;
        }
    }
    //Nullbyte exists in the front anyway
    char nullByte[1] = {0};
    if (count == 0){
        DataHelper::setNull(0, nullByte);
    }
    memcpy(data, nullByte, sizeof(char));

    float temp;
    switch(op){
        // valeu of MAX, MIN and SUM are all saved in the this->sum
        case MAX:
        case MIN:
        case SUM:
            temp = this->sum;
            memcpy((char*)data + sizeof(char), &temp, sizeof(float));
            break;
        case COUNT:
            // COUNT returns float as the requirement said
            temp = (float) this->count;
            memcpy((char*)data + sizeof(char), &temp, sizeof(float));
            break;
        case AVG:
            temp = (this->count == 0 ? 0 : this->sum / this->count);
            memcpy((char*)data + sizeof(char), &temp, sizeof(float));
            break;
    }

    this->finished = true;
    free(tuple);
    free(key);
    return 0;
}



// ... the rest of your implementations go here

bool QEDataHelper::isSatisfy(char *data, const std::vector<Attribute> &attrs, const Condition& cond) {
    int pivot = ceil((double)attrs.size() / 8);
    char* nullBytes = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(nullBytes, data, pivot);
    // Found such condition attribute in data or not

    // TODO: Maybe the condition attribute can be null

    if(cond.bRhsIsAttr == false){
        for(int i = 0 ; i < attrs.size(); i++){
            if(DataHelper::isNull(i, nullBytes))
                continue;
            if(attrs[i].name == cond.lhsAttr) {
                break;
            }
            if(attrs[i].type == TypeVarChar){
                int strLength;
                memcpy(&strLength, data + pivot, sizeof(int));
                pivot += (sizeof(int) + strLength);
            }
            else{
                pivot += sizeof(int);
            }
        }
        // save the key
        char* key = (char*)calloc(PAGE_SIZE, sizeof(char));
        if(cond.rhsValue.type == TypeVarChar){
            int strLength;
            memcpy(&strLength, data + pivot, sizeof(int));
            memcpy(key, data + pivot, strLength + sizeof(int));
        }
        else{
            memcpy(key, data + pivot, sizeof(int));
        }
        bool satisfy = DataHelper::isCompareSatisfy(key, cond.rhsValue.data, cond.op, cond.rhsValue.type);
        int k;
        memcpy(&k, key, sizeof(int));
        free(key);
        free(nullBytes);
        return satisfy;
    }

    return false;
}

bool JoinHelper::isSatisfy(char* leftData, char* rightData, const std::vector<Attribute>& leftAttrs, const std::vector<Attribute>& rightAttrs, const Condition& cond){
    char* leftKey = (char*) calloc(PAGE_SIZE, sizeof(char));
    char* rightKey = (char*) calloc(PAGE_SIZE, sizeof(char));

    //assuming have two attributes to compare for joining
    if(cond.bRhsIsAttr == true){
        //find the key from left and right data
        AttrType attrtype = JoinHelper::findKey(leftKey, leftData, leftAttrs, cond.lhsAttr);
        JoinHelper::findKey(rightKey, rightData, rightAttrs, cond.rhsAttr);

        bool result = DataHelper::isCompareSatisfy(leftKey, rightKey, cond.op, attrtype);

        free(leftKey);
        free(rightKey);
        return result;
    }

    free(leftKey);
    free(rightKey);
    return false;
}

//find the key value with the input attribute name
//return the attribute type
AttrType JoinHelper::findKey(char* key, char* data, const std::vector<Attribute>& attrs, const std::string name){
    int pivot = ceil((double)attrs.size() / 8);
    char* nullBytes = (char*)calloc(PAGE_SIZE, sizeof(char));
    memcpy(nullBytes, data, pivot);

    //assuming always exists
    for(int i = 0 ; i < attrs.size(); i++){
        //found
        if(attrs[i].name == name) {
            if(!DataHelper::isNull(i, nullBytes)){
                if(attrs[i].type == TypeVarChar){
                    int strLength;
                    memcpy(&strLength, data + pivot, sizeof(int));
                    memcpy(key, data + pivot, strLength + sizeof(int));
                }
                else{
                    memcpy(key, data + pivot, sizeof(int));
                }
                free(nullBytes);
                return attrs[i].type;

            } else {
                //assumed no null value existed
            }
        }
        // skip this one if is null
        if (DataHelper::isNull(i, nullBytes)){
            continue;
        }
        
        //not found
        if(attrs[i].type == TypeVarChar){
            int strLength;
            memcpy(&strLength, data + pivot, sizeof(int));
            pivot += (sizeof(int) + strLength);
        }
        else{
            pivot += sizeof(int);
        }
    }
}

//find the type of the attribute with the input name
AttrType JoinHelper::findType(const std::vector<Attribute>& attrs, const std::string name) {
    for (int i = 0; i < attrs.size(); i++) {
        // assume exists
        if (attrs[i].name == name) {
            return attrs[i].type;
        }
    }
}

void JoinHelper::join(char* result, char* leftData, char* rightData, const std::vector<Attribute>& leftAttrs, const std::vector<Attribute>& rightAttrs){
    char* oldNullBytes = (char*)calloc(PAGE_SIZE, sizeof(char));
    char* newNullBytes = (char*)calloc(PAGE_SIZE, sizeof(char));
    int newNullBytesLength = ceil(((double)leftAttrs.size() + rightAttrs.size()) / 8);
    int newPivot = newNullBytesLength;
    int strLength = 0;
    int totalCount = 0;

    //copy the left part in to the result, nullbyte is specially handled
    int oldPivot = ceil((double)leftAttrs.size() / 8);
    memcpy(oldNullBytes, leftData, oldPivot);
    for(int i = 0; i < leftAttrs.size(); i++){
        if(DataHelper::isNull(i, oldNullBytes)){
            DataHelper::setNull(totalCount, newNullBytes);
        } else if(leftAttrs.at(i).type == TypeVarChar){
            memcpy(&strLength, leftData + oldPivot, sizeof(int));
            memcpy(result + newPivot, leftData + oldPivot, strLength + sizeof(int));
            newPivot += strLength + sizeof(int);
            oldPivot += strLength + sizeof(int);
        }
        else{
            memcpy(result + newPivot, leftData + oldPivot, sizeof(int));
            oldPivot += sizeof(int);
            newPivot += sizeof(int);
        }
        totalCount++;
    }

    //copy the right part into the result, nullbyte is specially handled
    oldPivot = ceil((double)rightAttrs.size() / 8);
    memcpy(oldNullBytes, rightData, oldPivot);
    for(int i = 0; i < rightAttrs.size(); i++){
        if(DataHelper::isNull(i, oldNullBytes)){
            DataHelper::setNull(totalCount, newNullBytes);
        } else if(rightAttrs.at(i).type == TypeVarChar){
            memcpy(&strLength, rightData + oldPivot, sizeof(int));
            memcpy(result + newPivot, rightData + oldPivot, strLength + sizeof(int));
            newPivot += strLength + sizeof(int);
            oldPivot += strLength + sizeof(int);
        }
        else{
            memcpy(result + newPivot, rightData + oldPivot, sizeof(int));
            oldPivot += sizeof(int);
            newPivot += sizeof(int);
        }
        totalCount++;
    }

    //copy the nullbyte into the front of result
    memcpy(result, newNullBytes, newNullBytesLength);

    free(oldNullBytes);
    free(newNullBytes);
}

